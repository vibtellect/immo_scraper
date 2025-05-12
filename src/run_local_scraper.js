/**
 * Lokaler Ausführer für den Bazaraki-Scraper
 * 
 * Dieses Skript führt den Bazaraki-Scraper lokal aus und stellt sicher,
 * dass Telegram-Benachrichtigungen gesendet werden, auch beim ersten Lauf.
 */

// Importiere den Scraper
const { handler, testOptimizedScraping } = require('./bazaraki_lambda_scraper');

// Setze Umgebungsvariablen
process.env.IS_LOCAL = 'true';
process.env.FORCE_NOTIFICATION = 'true'; // Erzwingt Benachrichtigungen auch beim ersten Lauf
process.env.S3_BUCKET_NAME = 'bazaraki-scraper-results'; // Muss mit dem Standard-Bucket-Namen im Scraper übereinstimmen

// WICHTIG: Hier deine eigenen Telegram-Werte eintragen
// Diese werden für die Benachrichtigungen benötigt
process.env.TELEGRAM_BOT_TOKEN = ''; // Hier dein Bot-Token eintragen
process.env.TELEGRAM_CHAT_ID = ''; // Hier deine Chat-ID eintragen

// Überprüfe Telegram-Konfiguration
if (!process.env.TELEGRAM_BOT_TOKEN || !process.env.TELEGRAM_CHAT_ID) {
  console.log('⚠️ WARNUNG: Telegram-Konfiguration fehlt!');
  console.log('Führe den Scraper im Test-Modus ohne Telegram-Benachrichtigungen aus.');
  console.log('Für echte Benachrichtigungen:');
  console.log('1. Öffne src/run_local_scraper.js');
  console.log('2. Trage dein TELEGRAM_BOT_TOKEN und TELEGRAM_CHAT_ID ein');
  
  // Setze eine Test-Konfiguration
  process.env.TELEGRAM_BOT_TOKEN = 'test-token';
  process.env.TELEGRAM_CHAT_ID = 'test-chat-id';
  process.env.SKIP_TELEGRAM = 'true'; // Überspringe tatsächliche Telegram-Anfragen
}

// Konfiguration für den Scraper
const event = {
  // Standardfilter für Immobilien
  filters: {
    propertyTypes: ['apartments-flats', 'houses'],
    priceMax: '1500',
    cities: ['limassol', 'paphos'],
    bedrooms: '2-3'
  },
  // Erzwinge Benachrichtigungen
  forceNotification: true,
  // Debug-Modus aktivieren
  debug: true
};

// Führe den Scraper aus
async function runScraper() {
  console.log('🚀 Starte lokalen Bazaraki-Scraper...');
  console.log('📋 Konfiguration:');
  console.log(JSON.stringify(event, null, 2));
  
  try {
    // Führe den Lambda-Handler aus
    const result = await handler(event, {});
    console.log('✅ Scraper erfolgreich ausgeführt!');
    console.log(result);
  } catch (error) {
    console.error('❌ Fehler beim Ausführen des Scrapers:', error);
  }
}

// Starte den Scraper
runScraper().catch(error => {
  console.error('Unbehandelter Fehler:', error);
  process.exit(1);
});
