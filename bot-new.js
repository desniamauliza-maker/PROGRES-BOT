require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const { google } = require('googleapis');

// === ENV VARIABLES ===
const TOKEN = process.env.TELEGRAM_TOKEN;
const SHEET_ID = process.env.SHEET_ID;
let GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;

// Validasi
if (!TOKEN) {
  console.error('❌ TELEGRAM_TOKEN not set');
  process.exit(1);
}
if (!SHEET_ID) {
  console.error('❌ SHEET_ID not set');
  process.exit(1);
}
if (!GOOGLE_SERVICE_ACCOUNT_JSON) {
  console.error('❌ GOOGLE_SERVICE_ACCOUNT_JSON not set');
  process.exit(1);
}

// === PARSE GOOGLE SERVICE ACCOUNT ===
let serviceAccount;
try {
  let keyData = GOOGLE_SERVICE_ACCOUNT_JSON.trim();
  
  // Jika base64, decode dulu
  if (!keyData.startsWith('{')) {
    try {
      keyData = Buffer.from(keyData, 'base64').toString('utf-8');
    } catch (e) {
      // bukan base64
    }
  }
  
  serviceAccount = JSON.parse(keyData);
  console.log('✅ Google Service Account parsed');
} catch (e) {
  console.error('❌ Failed to parse JSON:', e.message);
  process.exit(1);
}

const auth = new google.auth.GoogleAuth({
  credentials: serviceAccount,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});
const sheets = google.sheets({ version: 'v4', auth });

const PROGRES_SHEET = 'PROGRES PSB';
const MASTER_SHEET = 'MASTER';  // Changed from USER_SHEET

// === CACHING LAYER ===
const cache = {
  masterData: null,
  masterDataTime: 0,
  progresData: null,
  progresDataTime: 0,
  cacheExpiry: 5 * 60 * 1000, // 5 menit
};

// === HELPER: Get sheet data with caching ===
async function getSheetData(sheetName, useCache = true) {
  try {
    // Cek cache untuk MASTER_SHEET (sering diquery)
    if (useCache && sheetName === MASTER_SHEET && cache.masterData) {
      if (Date.now() - cache.masterDataTime < cache.cacheExpiry) {
        console.log('📦 Using cached MASTER_SHEET');
        return cache.masterData;
      }
    }

    // Cek cache untuk PROGRES_SHEET
    if (useCache && sheetName === PROGRES_SHEET && cache.progresData) {
      if (Date.now() - cache.progresDataTime < cache.cacheExpiry) {
        console.log('📦 Using cached PROGRES_SHEET');
        return cache.progresData;
      }
    }

    // Fetch dari API
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000); // 15 detik timeout

    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: sheetName,
    });

    clearTimeout(timeout);
    const data = res.data.values || [];

    // Cache hasil
    if (sheetName === MASTER_SHEET) {
      cache.masterData = data;
      cache.masterDataTime = Date.now();
    } else if (sheetName === PROGRES_SHEET) {
      cache.progresData = data;
      cache.progresDataTime = Date.now();
    }

    return data;
  } catch (error) {
    console.error(`Error reading ${sheetName}:`, error.message);
    // Return cache meskipun expired jika API error
    if (sheetName === MASTER_SHEET && cache.masterData) {
      console.log('⚠️ API error, fallback ke cached MASTER_SHEET');
      return cache.masterData;
    }
    if (sheetName === PROGRES_SHEET && cache.progresData) {
      console.log('⚠️ API error, fallback ke cached PROGRES_SHEET');
      return cache.progresData;
    }
    throw error;
  }
}

// === HELPER: Append to sheet ===
async function appendSheetData(sheetName, values) {
  try {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: sheetName,
      valueInputOption: 'USER_ENTERED',
      resource: { values: [values] },
    });
  } catch (error) {
    console.error(`Error writing to ${sheetName}:`, error.message);
    throw error;
  }
}

// === HELPER: Send Telegram ===
async function sendTelegram(chatId, text, options = {}) {
  const maxLength = 4000;
  try {
    if (text.length <= maxLength) {
      return await bot.sendMessage(chatId, text, { parse_mode: 'HTML', ...options });
    } else {
      const lines = text.split('\n');
      let chunk = '';
      for (let i = 0; i < lines.length; i++) {
        if ((chunk + lines[i] + '\n').length > maxLength) {
          await bot.sendMessage(chatId, chunk, { parse_mode: 'HTML', ...options });
          chunk = '';
        }
        chunk += lines[i] + '\n';
      }
      if (chunk.trim()) {
        await bot.sendMessage(chatId, chunk, { parse_mode: 'HTML', ...options });
      }
    }
  } catch (error) {
    console.error('Error sending message:', error.message);
  }
}

// === HELPER: Wrap with timeout ===
function withTimeout(promise, ms = 10000) {
  return Promise.race([
    promise,
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Timeout - Google API response too slow')), ms)
    )
  ]);
}

// === HELPER: Get user role ===
async function getUserRole(username) {
  try {
    const data = await getSheetData(MASTER_SHEET);
    for (let i = 1; i < data.length; i++) {
      const sheetUser = (data[i][8] || '').replace('@', '').toLowerCase().trim();
      const inputUser = (username || '').replace('@', '').toLowerCase().trim();
      const status = (data[i][10] || '').toUpperCase().trim();
      const role = (data[i][9] || '').toUpperCase().trim();
      
      if (sheetUser === inputUser && status === 'AKTIF') {
        return role;
      }
    }
    return null;
  } catch (error) {
    console.error('Error getting user role:', error.message);
    return null;
  }
}

// === HELPER: Get user data ===
async function getUserData(username) {
  try {
    const data = await getSheetData(MASTER_SHEET);
    for (let i = 1; i < data.length; i++) {
      const sheetUser = (data[i][8] || '').replace('@', '').toLowerCase().trim();
      const inputUser = (username || '').replace('@', '').toLowerCase().trim();
      const status = (data[i][10] || '').toUpperCase().trim();
      
      if (sheetUser === inputUser && status === 'AKTIF') {
        return data[i];
      }
    }
    return null;
  } catch (error) {
    console.error('Error getting user:', error.message);
    return null;
  }
}

// === HELPER: Check authorization ===
async function checkAuthorization(username, requiredRoles = []) {
  try {
    const userRole = await withTimeout(getUserRole(username), 8000);
    if (!userRole) {
      return { authorized: false, role: null, message: '❌ Anda tidak terdaftar di sistem.' };
    }
    
    if (requiredRoles.length > 0 && !requiredRoles.includes(userRole)) {
      return { authorized: false, role: userRole, message: `❌ Akses ditolak. Role ${userRole} tidak memiliki izin untuk command ini.` };
    }
    
    return { authorized: true, role: userRole };
  } catch (error) {
    console.error('Authorization error:', error.message);
    return { authorized: false, role: null, message: '❌ Terjadi kesalahan saat verifikasi. Server sedang sibuk.' };
  }
}

// === HELPER: Get valid symptom values from MASTER sheet (UPDATE LAPANGAN columns A-D) ===
async function getValidSymptoms() {
  try {
    const data = await getSheetData(MASTER_SHEET);
    const categories = {
      'KENDALA TEKNIK (NON INSC)': [],
      'KENDALA TEKNIK (INSC)': [],
      'KENDALA PELANGGAN': [],
      'POTENSI PS': [],
    };
    const categoryKeys = Object.keys(categories);
    const allValues = [];

    // Baris ke-3+ (index 2+), kolom A-D (index 0-3) berisi nilai UPDATE LAPANGAN
    for (let i = 2; i < data.length; i++) {
      for (let col = 0; col < 4; col++) {
        const val = (data[i][col] || '').trim().toUpperCase();
        if (val && !allValues.includes(val)) {
          allValues.push(val);
          categories[categoryKeys[col]].push(val);
        }
      }
    }

    return { allValues, categories };
  } catch (error) {
    console.error('Error getting valid symptoms:', error.message);
    return { allValues: [], categories: {} };
  }
}

// === HELPER: Validate symptom against master list ===
async function validateSymptom(symptomValue) {
  const { allValues, categories } = await withTimeout(getValidSymptoms(), 8000);
  const normalized = (symptomValue || '').trim().toUpperCase();

  if (!normalized) {
    return { valid: false, isEmpty: true, categories };
  }

  if (allValues.includes(normalized)) {
    return { valid: true, normalized };
  }

  return { valid: false, isEmpty: false, categories };
}

// === HELPER: Format symptom error message ===
function formatSymptomError(categories) {
  let msg = '❌ SYMPTOM tidak valid!\n\n';
  msg += '📋 <b>Pilih salah satu dari daftar berikut:</b>\n\n';

  for (const [catName, values] of Object.entries(categories)) {
    if (values.length > 0) {
      msg += `<b>▸ ${catName}:</b>\n`;
      values.forEach(v => {
        msg += `  • ${v}\n`;
      });
      msg += '\n';
    }
  }

  msg += '<i>Pastikan SYMPTOM sesuai dengan daftar di atas.</i>';
  return msg;
}

// === HELPER: Parse progres data ===
function parseProgres(text, userRow, username) {
  let data = {
    channel: '',
    scOrderNo: '',
    serviceNo: '',
    customerName: '',
    workzone: '',
    contactPhone: '',
    odp: '',
    memo: '',
    symptom: '',
    ao: '',
    workorder: '',
    tikor: '',
    snOnt: '',
    nikOnt: '',
    stbId: '',
    nikStb: '',
    dateCreated: new Date().toLocaleDateString('id-ID', {
      weekday: 'long',
      day: 'numeric',
      month: 'long',
      year: 'numeric',
      timeZone: 'Asia/Jakarta',
    }),
    teknisi: (username || '').replace('@', ''),
  };

  const patterns = {
    channel: /CHANNEL\s*:\s*([A-Za-z0-9]+)/i,
    scOrderNo: /SC\s*ORDER\s*NO\s*:\s*(.+?)(?=\n|$)/i,
    serviceNo: /SERVICE\s*NO\s*:\s*([0-9]+)/i,
    customerName: /CUSTOMER\s*NAME\s*:\s*(.+?)(?=\n|$)/i,
    workzone: /WORKZONE\s*:\s*([A-Za-z0-9]+)/i,
    contactPhone: /CONTACT\s*PHONE\s*:\s*([0-9\+\-\s]+)/i,
    odp: /ODP\s*:\s*(.+?)(?=\n|$)/i,
    memo: /MEMO\s*:\s*(.+?)(?=\n|$)/i,
    symptom: /SYMPTOM\s*:\s*(.+?)(?=\n|$)/i,
    ao: /AO\s*:\s*(.+?)(?=\n|$)/i,
    workorder: /WORKORDER\s*:\s*([A-Za-z0-9]+)/i,
    tikor: /TIKOR\s*:\s*(.+?)(?=\n|$)/i,
    snOnt: /SN\s*ONT\s*:\s*(.+?)(?=\n|$)/i,
    nikOnt: /NIK\s*ONT\s*:\s*([0-9]+)/i,
    stbId: /STB\s*ID\s*:\s*(.+?)(?=\n|$)/i,
    nikStb: /NIK\s*STB\s*:\s*([0-9]+)/i,
  };

  for (const [key, pattern] of Object.entries(patterns)) {
    const match = text.match(pattern);
    if (match && match[1]) {
      data[key] = match[1].trim();
    }
  }

  return data;
}

// === HELPER: Parse aktivasi data ===
function parseAktivasi(text, username) {
  let data = {
    channel: '',
    dateCreated: '',
    scOrderNo: '',
    workorder: '',
    ao: '',
    ncli: '',
    serviceNo: '',
    address: '',
    customerName: '',
    workzone: '',
    contactPhone: '',
    bookingDate: '',
    paket: '',
    package: '',
    odp: '',
    mitra: '',
    symptom: '',
    memo: '',
    tikor: '',
    snOnt: '',
    nikOnt: '',
    stbId: '',
    nikStb: '',
    teknisi: (username || '').replace('@', ''),
  };

  const patterns = {
    channel: /CHANNEL\s*:\s*(.+?)(?=\n|$)/i,
    dateCreated: /DATE\s*CREATED\s*:\s*(.+?)(?=\n|$)/i,
    scOrderNo: /SC\s*ORDER\s*NO\s*:\s*(.+?)(?=\n|$)/i,
    workorder: /WORKORDER\s*:\s*(.+?)(?=\n|$)/i,
    ao: /AO\s*:\s*(.+?)(?=\n|$)/i,
    ncli: /NCLI\s*:\s*(.+?)(?=\n|$)/i,
    serviceNo: /SERVICE\s*NO\s*:\s*(.+?)(?=\n|$)/i,
    address: /ADDRESS\s*:\s*(.+?)(?=\n|$)/i,
    customerName: /CUSTOMER\s*NAME\s*:\s*(.+?)(?=\n|$)/i,
    workzone: /WORKZONE\s*:\s*(.+?)(?=\n|$)/i,
    contactPhone: /CONTACT\s*PHONE\s*:\s*(.+?)(?=\n|$)/i,
    bookingDate: /BOOKING\s*DATE\s*:\s*(.+?)(?=\n|$)/i,
    paket: /PAKET\s*:\s*(.+?)(?=\n|$)/i,
    package: /PACKAGE\s*:\s*(.+?)(?=\n|$)/i,
    odp: /ODP\s*:\s*(.+?)(?=\n|$)/i,
    mitra: /MITRA\s*:\s*(.+?)(?=\n|$)/i,
    symptom: /SYMPTOM\s*:\s*(.+?)(?=\n|$)/i,
    memo: /MEMO\s*:\s*(.+?)(?=\n|$)/i,
    tikor: /TIKOR\s*:\s*(.+?)(?=\n|$)/i,
    snOnt: /SN\s*ONT\s*:\s*(.+?)(?=\n|$)/i,
    nikOnt: /NIK\s*ONT\s*:\s*(.+?)(?=\n|$)/i,
    stbId: /STB\s*ID\s*:\s*(.+?)(?=\n|$)/i,
    nikStb: /NIK\s*STB\s*:\s*(.+?)(?=\n|$)/i,
  };

  for (const [key, pattern] of Object.entries(patterns)) {
    const match = text.match(pattern);
    if (match && match[1]) {
      data[key] = match[1].trim();
    }
  }

  return data;
}

// === BOT SETUP ===
const PORT = process.env.PORT || 3001;
const RAILWAY_STATIC_URL = process.env.RAILWAY_STATIC_URL;
const USE_WEBHOOK = !!RAILWAY_STATIC_URL;

let bot;

if (USE_WEBHOOK) {
  const express = require('express');
  const app = express();
  app.use(express.json());

  bot = new TelegramBot(TOKEN);
  const webhookUrl = `https://${RAILWAY_STATIC_URL}/progres${TOKEN}`;

  bot.setWebHook(webhookUrl).then(() => {
    console.log(`✅ Webhook set: ${webhookUrl}`);
  }).catch(err => {
    console.error('❌ Webhook error:', err.message);
  });

  app.post(`/progres${TOKEN}`, (req, res) => {
    bot.processUpdate(req.body);
    res.sendStatus(200);
  });

  app.get('/', (req, res) => {
    res.send('Bot Progres PSB is running!');
  });

  app.listen(PORT, () => {
    console.log(`✅ Server running on port ${PORT}`);
  });
} else {
  // Polling mode dengan optimized settings untuk multiple groups
  bot = new TelegramBot(TOKEN, { 
    polling: {
      interval: 300,        // Check every 300ms (faster response)
      autoStart: true,
      params: {
        timeout: 10,        // Keep-alive timeout
        allowed_updates: ['message']  // Only get message updates
      }
    }
  });
  console.log('✅ Bot running in polling mode (optimized for multiple groups)');
  
  // Error handler untuk polling
  bot.on('polling_error', (error) => {
    if (error.code === 'EFATAL') {
      console.error('❌ Polling fatal error:', error.message);
    } else {
      console.error('⚠️ Polling error:', error.message);
    }
  });
}

// === MESSAGE HANDLER ===
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const msgId = msg.message_id;
  const text = (msg.text || '').trim();
  const username = msg.from.username || '';
  const groupName = msg.chat.title || msg.chat.first_name || `ID:${chatId}`;
  const groupType = msg.chat.type; // 'group', 'supergroup', 'private'

  // Early return untuk pesan kosong atau non-text
  if (!text) {
    return;
  }

  // Early return untuk pesan yang bukan command
  if (!text.startsWith('/')) {
    return;
  }

  console.log(`📨 [${groupType}] ${groupName} | [@${username}] ${text.substring(0, 60)}`);

  try {
    // === /UPDATE ===
    if (/^\/UPDATE\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username, ['USER', 'ADMIN']);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const inputText = text.replace(/^\/UPDATE\s*/i, '').trim();
        if (!inputText) {
          return sendTelegram(chatId, '❌ Silakan kirim data progres setelah /UPDATE.', { reply_to_message_id: msgId });
        }

        const user = await withTimeout(getUserData(username), 8000);
        const parsed = parseProgres(inputText, user, username);

        const required = ['channel', 'scOrderNo', 'serviceNo', 'customerName', 'workzone', 'symptom'];
        const missing = required.filter(f => !parsed[f]);

        if (missing.length > 0) {
          return sendTelegram(chatId, `❌ Field wajib: ${missing.join(', ')}`, { reply_to_message_id: msgId });
        }

        // Validasi SYMPTOM terhadap master UPDATE LAPANGAN
        const symptomCheck = await validateSymptom(parsed.symptom);
        if (!symptomCheck.valid) {
          return sendTelegram(chatId, formatSymptomError(symptomCheck.categories), { reply_to_message_id: msgId });
        }
        parsed.symptom = symptomCheck.normalized; // Normalize ke uppercase

        const row = [
          parsed.dateCreated,    // A: DATE CREATED
          parsed.channel,        // B: CHANNEL
          parsed.workorder,      // C: WORKORDER
          parsed.ao,             // D: AO
          parsed.scOrderNo,      // E: SC ORDER NO
          parsed.serviceNo,      // F: SERVICE NO
          parsed.customerName,   // G: CUSTOMER NAME
          parsed.workzone,       // H: WORKZONE
          parsed.contactPhone,   // I: CONTACT PHONE
          parsed.odp,            // J: ODP
          parsed.symptom,        // K: SYMPTOM
          parsed.memo,           // L: MEMO
          parsed.tikor,          // M: TIKOR
          parsed.snOnt,          // N: SN ONT
          parsed.nikOnt,         // O: NIK ONT
          parsed.stbId,          // P: STB ID
          parsed.nikStb,         // Q: NIK STB
          parsed.teknisi,        // R: NAMA TELEGRAM TEKNISI
        ];

        await withTimeout(appendSheetData(PROGRES_SHEET, row), 10000);

        let confirmMsg = '✅ Data berhasil disimpan!\n\n';

        return sendTelegram(chatId, confirmMsg, { reply_to_message_id: msgId });
      } catch (updateErr) {
        console.error('❌ /UPDATE Error:', updateErr.message);
        return sendTelegram(chatId, `❌ Error: ${updateErr.message}. Silakan coba lagi.`, { reply_to_message_id: msgId });
      }
    }

    // === /AKTIVASI ===
    else if (/^\/AKTIVASI\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username, ['USER', 'ADMIN']);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const inputText = text.replace(/^\/AKTIVASI\s*/i, '').trim();
        if (!inputText) {
          return sendTelegram(chatId, '❌ Silakan kirim data aktivasi setelah /AKTIVASI.', { reply_to_message_id: msgId });
        }

        const parsed = parseAktivasi(inputText, username);
        console.log('✅ Parsed aktivasi data:', parsed);

        const required = ['channel', 'customerName', 'serviceNo', 'workzone', 'symptom'];
        const missing = required.filter(f => !parsed[f]);

        if (missing.length > 0) {
          return sendTelegram(chatId, `❌ Field wajib: ${missing.join(', ')}`, { reply_to_message_id: msgId });
        }

        // Validasi SYMPTOM terhadap master UPDATE LAPANGAN
        const symptomCheck = await validateSymptom(parsed.symptom);
        if (!symptomCheck.valid) {
          return sendTelegram(chatId, formatSymptomError(symptomCheck.categories), { reply_to_message_id: msgId });
        }
        parsed.symptom = symptomCheck.normalized; // Normalize ke uppercase

        // Tentukan package (gunakan preferensi PACKAGE untuk DIGIPOS, PAKET untuk BS/ES/GS)
        const packageInfo = parsed.package || parsed.paket || '-';

        const row = [
          new Date().toLocaleDateString('id-ID', {
            weekday: 'long',
            day: 'numeric',
            month: 'long',
            year: 'numeric',
            timeZone: 'Asia/Jakarta',
          }),                       // A: DATE CREATED
          parsed.channel,           // B: CHANNEL
          parsed.workorder,         // C: WORKORDER
          parsed.ao,                // D: AO
          parsed.scOrderNo,         // E: SC ORDER NO
          parsed.serviceNo,         // F: SERVICE NO
          parsed.customerName,      // G: CUSTOMER NAME
          parsed.workzone,          // H: WORKZONE
          parsed.contactPhone,      // I: CONTACT PHONE
          parsed.odp,               // J: ODP
          parsed.symptom,           // K: SYMPTOM
          parsed.memo,              // L: MEMO
          parsed.tikor,             // M: TIKOR
          parsed.snOnt,             // N: SN ONT
          parsed.nikOnt,            // O: NIK ONT
          parsed.stbId,             // P: STB ID
          parsed.nikStb,            // Q: NIK STB
          parsed.teknisi,           // R: NAMA TELEGRAM TEKNISI
        ];

        console.log('📝 Row data to append:', row);
        await withTimeout(appendSheetData(PROGRES_SHEET, row), 10000);

        let confirmMsg = '✅ Data aktivasi berhasil disimpan!\n\n';

        return sendTelegram(chatId, confirmMsg, { reply_to_message_id: msgId });
      } catch (aktivasiErr) {
        console.error('❌ /AKTIVASI Error:', aktivasiErr.message);
        console.error('Stack:', aktivasiErr.stack);
        return sendTelegram(chatId, `❌ Error: ${aktivasiErr.message}. Silakan coba lagi.`, { reply_to_message_id: msgId });
      }
    }

    // === /today [TEKNISI] ===
    else if (/^\/today\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username, ['ADMIN']);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const args = text.replace(/^\/today\s*/i, '').trim();
        if (!args) {
          return sendTelegram(chatId, '❌ Format: /today TEKNISI_ID', { reply_to_message_id: msgId });
        }

        const today = new Date().toLocaleDateString('id-ID', {
          weekday: 'long',
          day: 'numeric',
          month: 'long',
          year: 'numeric',
          timeZone: 'Asia/Jakarta',
        });

        const data = await withTimeout(getSheetData(PROGRES_SHEET), 10000);
        let map = {};

        for (let i = 1; i < data.length; i++) {
          const dateCreated = (data[i][0] || '').trim();  // Column A
          if (dateCreated !== today) continue;
          
          const teknisi = (data[i][17] || '-').trim();  // Column R
          if (teknisi !== args) continue;
          
          const symptom = (data[i][10] || '-').trim();  // Column K
          const ao = (data[i][3] || '-').trim();  // Column D (AO)

          if (!map[symptom]) map[symptom] = [];
          map[symptom].push(ao);
        }

        const entries = Object.entries(map)
          .sort((a, b) => b[1].length - a[1].length);

        let totalWO = Object.values(map).reduce((sum, arr) => sum + arr.length, 0);
        let msg = `📋 <b>PROGRES HARI INI - ${args}</b>\n\n`;
        msg += `<b>Total: ${totalWO} WO</b>\n`;
        
        if (entries.length === 0) {
          msg += '<i>Belum ada data untuk hari ini</i>';
        } else {
          entries.forEach((entry) => {
            const [symptom, aos] = entry;
            msg += `   • <b>${symptom}: ${aos.length}</b>\n`;
            aos.forEach(ao => {
              msg += `${ao}\n`;
            });
            msg += '\n';
          });
        }

        return sendTelegram(chatId, msg, { reply_to_message_id: msgId });
      } catch (err) {
        console.error('❌ /today Error:', err.message);
        return sendTelegram(chatId, `❌ Error: ${err.message}. Server sedang sibuk.`, { reply_to_message_id: msgId });
      }
    }

    // === /all [TEKNISI] ===
    else if (/^\/all\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username, ['ADMIN']);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const args = text.replace(/^\/all\s*/i, '').trim();
        if (!args) {
          return sendTelegram(chatId, '❌ Format: /all TEKNISI_ID', { reply_to_message_id: msgId });
        }

        const data = await withTimeout(getSheetData(PROGRES_SHEET), 10000);
      let map = {};

      for (let i = 1; i < data.length; i++) {
        const teknisi = (data[i][17] || '-').trim();  // Column R
        if (teknisi !== args) continue;
        
        const symptom = (data[i][10] || '-').trim();  // Column K
        const ao = (data[i][3] || '-').trim();  // Column D (AO)

        if (!map[symptom]) map[symptom] = [];
        map[symptom].push(ao);
      }

      const entries = Object.entries(map)
        .sort((a, b) => b[1].length - a[1].length);

      let totalWO = Object.values(map).reduce((sum, arr) => sum + arr.length, 0);
      let msg = `📋 <b>SELURUH PROGRES - ${args}</b>\n\n`;
      msg += `<b>Total: ${totalWO} WO</b>\n`;
      
      if (entries.length === 0) {
        msg += '<i>Belum ada data</i>';
      } else {
        entries.forEach((entry) => {
          const [symptom, aos] = entry;
          msg += `   • <b>${symptom}: ${aos.length}</b>\n`;
          aos.forEach(ao => {
            msg += `${ao}\n`;
          });
          msg += '\n';
        });
      }

      return sendTelegram(chatId, msg, { reply_to_message_id: msgId });
      } catch (err) {
        console.error('❌ /all Error:', err.message);
        return sendTelegram(chatId, `❌ Error: ${err.message}. Server sedang sibuk.`, { reply_to_message_id: msgId });
      }
    }

    // === /progres ===
    else if (/^\/progres\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username, ['ADMIN']);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const today = new Date().toLocaleDateString('id-ID', {
          weekday: 'long',
          day: 'numeric',
          month: 'long',
          year: 'numeric',
          timeZone: 'Asia/Jakarta',
        });

        const data = await withTimeout(getSheetData(PROGRES_SHEET), 10000);
      let map = {};

      for (let i = 1; i < data.length; i++) {
        const dateCreated = (data[i][0] || '').trim();  // Column A (index 0)
        if (dateCreated !== today) continue;  // Filter hanya hari ini
        
        const teknisi = (data[i][17] || '-').trim();  // Column R (index 17)
        const symptom = (data[i][10] || '-').trim();  // Column K (index 10)

        if (!map[teknisi]) map[teknisi] = { total: 0 };
        map[teknisi].total++;
        map[teknisi][symptom] = (map[teknisi][symptom] || 0) + 1;
      }

      const entries = Object.entries(map)
        .sort((a, b) => b[1].total - a[1].total);

      let msg = `📊 <b>LAPORAN TEKNISI - HARI INI</b>\n<b>${today}</b>\n\n`;
      
      if (entries.length === 0) {
        msg += '<i>Belum ada data untuk hari ini</i>';
      } else {
        entries.forEach((entry) => {
          const [teknisi, counts] = entry;
          msg += `🔸 <b>${teknisi}</b>\n`;
          msg += `   <b>Total:</b> ${counts.total} WO\n`;
          
          const symptoms = Object.entries(counts)
            .filter(([k]) => k !== 'total')
            .sort((a, b) => b[1] - a[1]);
          
          symptoms.forEach(([symptomName, count]) => {
            msg += `   • ${symptomName}: ${count}\n`;
          });
          msg += '\n';
        });
      }

      return sendTelegram(chatId, msg, { reply_to_message_id: msgId });
      } catch (err) {
        console.error('❌ /progres Error:', err.message);
        return sendTelegram(chatId, `❌ Error: ${err.message}. Server sedang sibuk.`, { reply_to_message_id: msgId });
      }
    }

    // === /allprogres ===
    else if (/^\/allprogres\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username, ['ADMIN']);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const data = await withTimeout(getSheetData(PROGRES_SHEET), 10000);
      let map = {};

      for (let i = 1; i < data.length; i++) {
        const teknisi = (data[i][17] || '-').trim();  // Column R (index 17)
        const symptom = (data[i][10] || '-').trim();  // Column K (index 10)

        if (!map[teknisi]) map[teknisi] = { total: 0 };
        map[teknisi].total++;
        map[teknisi][symptom] = (map[teknisi][symptom] || 0) + 1;
      }

      const entries = Object.entries(map)
        .sort((a, b) => b[1].total - a[1].total);

      let msg = '📊 <b>LAPORAN TEKNISI - KESELURUHAN</b>\n\n';
      
      if (entries.length === 0) {
        msg += '<i>Belum ada data</i>';
      } else {
        entries.forEach((entry) => {
          const [teknisi, counts] = entry;
          msg += `🔸 <b>${teknisi}</b>\n`;
          msg += `   <b>Total:</b> ${counts.total} WO\n`;
          
          const symptoms = Object.entries(counts)
            .filter(([k]) => k !== 'total')
            .sort((a, b) => b[1] - a[1]);
          
          symptoms.forEach(([symptomName, count]) => {
            msg += `   • ${symptomName}: ${count}\n`;
          });
          msg += '\n';
        });
      }

      return sendTelegram(chatId, msg, { reply_to_message_id: msgId });
      } catch (err) {
        console.error('❌ /allprogres Error:', err.message);
        return sendTelegram(chatId, `❌ Error: ${err.message}. Server sedang sibuk.`, { reply_to_message_id: msgId });
      }
    }

    // === /cek ===
    else if (/^\/cek\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username, ['ADMIN']);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const today = new Date().toLocaleDateString('id-ID', {
          weekday: 'long',
          day: 'numeric',
          month: 'long',
          year: 'numeric',
          timeZone: 'Asia/Jakarta',
        });

        const data = await withTimeout(getSheetData(PROGRES_SHEET), 10000);
      let map = {};

      for (let i = 1; i < data.length; i++) {
        const dateCreated = (data[i][0] || '').trim();  // Column A (index 0)
        if (dateCreated !== today) continue;  // Filter hanya hari ini
        
        const workzone = (data[i][7] || '-').trim();   // Column H (index 7)
        const symptom = (data[i][10] || '-').trim();   // Column K (index 10)

        if (!map[workzone]) map[workzone] = { total: 0 };
        map[workzone].total++;
        map[workzone][symptom] = (map[workzone][symptom] || 0) + 1;
      }

      const entries = Object.entries(map)
        .sort((a, b) => b[1].total - a[1].total);

      let msg = `📍 <b>REKAP WORKZONE - HARI INI</b>\n<b>${today}</b>\n\n`;
      
      if (entries.length === 0) {
        msg += '<i>Belum ada data untuk hari ini</i>';
      } else {
        entries.forEach((entry) => {
          const [workzone, counts] = entry;
          msg += `🔸 <b>${workzone}</b>\n`;
          msg += `   <b>Total:</b> ${counts.total} WO\n`;
          
          const symptoms = Object.entries(counts)
            .filter(([k]) => k !== 'total')
            .sort((a, b) => b[1] - a[1]);
          
          symptoms.forEach(([symptomName, count]) => {
            msg += `   • ${symptomName}: ${count}\n`;
          });
          msg += '\n';
        });
      }

      return sendTelegram(chatId, msg, { reply_to_message_id: msgId });
      } catch (err) {
        console.error('❌ /cek Error:', err.message);
        return sendTelegram(chatId, `❌ Error: ${err.message}. Server sedang sibuk.`, { reply_to_message_id: msgId });
      }
    }

    // === /allcek ===
    else if (/^\/allcek\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username, ['ADMIN']);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const data = await withTimeout(getSheetData(PROGRES_SHEET), 10000);
      let map = {};

      for (let i = 1; i < data.length; i++) {
        const workzone = (data[i][7] || '-').trim();   // Column H (index 7)
        const symptom = (data[i][10] || '-').trim();   // Column K (index 10)

        if (!map[workzone]) map[workzone] = { total: 0 };
        map[workzone].total++;
        map[workzone][symptom] = (map[workzone][symptom] || 0) + 1;
      }

      const entries = Object.entries(map)
        .sort((a, b) => b[1].total - a[1].total);

      let msg = '📍 <b>REKAP WORKZONE - KESELURUHAN</b>\n\n';
      
      if (entries.length === 0) {
        msg += '<i>Belum ada data</i>';
      } else {
        entries.forEach((entry) => {
          const [workzone, counts] = entry;
          msg += `🔸 <b>${workzone}</b>\n`;
          msg += `   <b>Total:</b> ${counts.total} WO\n`;
          
          const symptoms = Object.entries(counts)
            .filter(([k]) => k !== 'total')
            .sort((a, b) => b[1] - a[1]);
          
          symptoms.forEach(([symptomName, count]) => {
            msg += `   • ${symptomName}: ${count}\n`;
          });
          msg += '\n';
        });
      }

      return sendTelegram(chatId, msg, { reply_to_message_id: msgId });
      } catch (err) {
        console.error('❌ /allcek Error:', err.message);
        return sendTelegram(chatId, `❌ Error: ${err.message}. Server sedang sibuk.`, { reply_to_message_id: msgId });
      }
    }

    // === /listsymptom ===
    else if (/^\/listsymptom\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username, ['USER', 'ADMIN']);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const { categories } = await withTimeout(getValidSymptoms(), 8000);
        let msg = '📋 <b>DAFTAR SYMPTOM YANG VALID</b>\n\n';

        for (const [catName, values] of Object.entries(categories)) {
          if (values.length > 0) {
            msg += `<b>▸ ${catName}:</b>\n`;
            values.forEach(v => {
              msg += `  • ${v}\n`;
            });
            msg += '\n';
          }
        }

        msg += '<i>Gunakan salah satu nilai di atas untuk field SYMPTOM.</i>';
        return sendTelegram(chatId, msg, { reply_to_message_id: msgId });
      } catch (err) {
        console.error('❌ /listsymptom Error:', err.message);
        return sendTelegram(chatId, `❌ Error: ${err.message}`, { reply_to_message_id: msgId });
      }
    }

    // === /help ===
    else if (/^\/(help|start)\b/i.test(text)) {
      try {
        const auth = await checkAuthorization(username);
        if (!auth.authorized) {
          return sendTelegram(chatId, auth.message, { reply_to_message_id: msgId });
        }

        const helpMsg = `🤖 Bot Progres PSB

Commands:
/UPDATE - Input progres (di group)
/AKTIVASI - Input data aktivasi (di group)
/today TEKNISI_ID - Progres hari ini (teknisi)
/all TEKNISI_ID - Seluruh progres (teknisi)
/progres - Laporan teknisi HARI INI
/allprogres - Laporan teknisi KESELURUHAN
/cek - Rekap workzone HARI INI
/allcek - Rekap workzone KESELURUHAN
/listsymptom - Daftar SYMPTOM yang valid
/help - Bantuan

⚠️ SYMPTOM harus sesuai daftar MASTER (kolom UPDATE LAPANGAN)
Gunakan /listsymptom untuk melihat daftar yang valid.

Contoh:
/today FH_ABDULLAH_16891190
/all FH_ABDULLAH_16891190
/progres
/allprogres
/cek
/allcek`;

        return sendTelegram(chatId, helpMsg, { reply_to_message_id: msgId });
      } catch (err) {
        console.error('❌ /help Error:', err.message);
        return sendTelegram(chatId, '❌ Terjadi kesalahan.', { reply_to_message_id: msgId });
      }
    }

  } catch (err) {
    console.error('Error:', err.message);
    sendTelegram(chatId, '❌ Terjadi kesalahan sistem.', { reply_to_message_id: msgId });
  }
});

process.on('unhandledRejection', (reason) => {
  console.error('Error:', reason);
});

console.log('\n🚀 Bot Progres PSB started!');
console.log(`Mode: ${USE_WEBHOOK ? 'Webhook' : 'Polling (Optimized)'}`);
console.log('═'.repeat(50));
console.log('✅ Multi-Group Support Enabled');
console.log('✅ Auto-Cache Enabled (5 min expiry)');
console.log('✅ Timeout Protection Enabled');
console.log('✅ Error Fallback Enabled');
console.log('═'.repeat(50));
