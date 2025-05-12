const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');

// Konfiguration
const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME || 'vibtellect-immo-scraper-results';
const DEFAULT_KEY = 'state.json'; // Die Haupt-Zustands-Datei

// Prüfen, ob wir in einer lokalen Umgebung oder in AWS Lambda sind
const isLocal = !process.env.AWS_LAMBDA_FUNCTION_NAME;
console.log(`Ausführungsumgebung: ${isLocal ? 'Lokal' : 'AWS Lambda'}`);

// AWS Konfiguration für lokale Ausführung
if (isLocal) {
  // Lokale AWS-Konfiguration
  AWS.config.update({
    region: 'eu-central-1', // Standard-Region
    // Für lokale Entwicklung - verwenden Sie Ihre eigenen Credentials oder konfigurieren Sie AWS CLI
    credentials: new AWS.SharedIniFileCredentials({ profile: 'default' })
  });
}

// S3-Client erstellen
const s3 = new AWS.S3();

// Funktion zum Herunterladen einer Datei aus S3
async function downloadFileFromS3(bucket, key, outputPath) {
  console.log(`Versuche, Datei '${key}' aus Bucket '${bucket}' herunterzuladen...`);
  
  try {
    // S3-Objekt abrufen
    const data = await s3.getObject({
      Bucket: bucket,
      Key: key
    }).promise();
    
    // Datei lokal speichern
    fs.writeFileSync(outputPath, data.Body.toString());
    
    console.log(`Datei erfolgreich heruntergeladen und gespeichert als: ${outputPath}`);
    console.log(`Inhalt: ${data.Body.toString().substring(0, 200)}... (gekürzt)`);
    
    return {
      success: true,
      data: JSON.parse(data.Body.toString())
    };
  } catch (error) {
    console.error(`Fehler beim Herunterladen der Datei: ${error.message}`);
    return {
      success: false,
      error: error.message
    };
  }
}

// Funktion zum Auflisten aller Dateien im Bucket
async function listFilesInBucket(bucket, prefix = '') {
  console.log(`Liste Dateien in Bucket '${bucket}' mit Prefix '${prefix || 'keinem'}'...`);
  
  try {
    const data = await s3.listObjectsV2({
      Bucket: bucket,
      Prefix: prefix
    }).promise();
    
    console.log(`${data.Contents.length} Dateien gefunden:`);
    
    // Sortiere nach letzter Änderung (neueste zuerst)
    const sortedFiles = [...data.Contents].sort(
      (a, b) => new Date(b.LastModified) - new Date(a.LastModified)
    );
    
    sortedFiles.forEach(file => {
      console.log(`- ${file.Key} (${file.Size} Bytes, zuletzt geändert: ${file.LastModified})`);
    });
    
    return {
      success: true,
      files: sortedFiles
    };
  } catch (error) {
    console.error(`Fehler beim Auflisten der Dateien: ${error.message}`);
    return {
      success: false,
      error: error.message
    };
  }
}

// Hauptfunktion
async function main() {
  const outputDir = path.join(__dirname, '..', 'downloads');
  
  // Ausgabeverzeichnis erstellen, falls es nicht existiert
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  
  // Mögliche Präfixe oder Ordner durchsuchen
  const prefixes = ['', 'results/', 'state/'];
  let filesFound = false;
  
  console.log('Durchsuche S3-Bucket nach allen verfügbaren Dateien...');
  
  for (const prefix of prefixes) {
    console.log(`\nSuche in Prefix: ${prefix || '(Wurzelverzeichnis)'}`);
    const result = await listFilesInBucket(S3_BUCKET_NAME, prefix);
    
    if (result.success && result.files.length > 0) {
      filesFound = true;
      
      // Die neueste Datei herunterladen
      if (result.files.length > 0) {
        const newestFile = result.files[0]; // Neueste Datei dank Sortierung
        console.log(`\nLade neueste Datei herunter: ${newestFile.Key}`);
        
        const outputPath = path.join(outputDir, path.basename(newestFile.Key));
        await downloadFileFromS3(S3_BUCKET_NAME, newestFile.Key, outputPath);
      }
    }
  }
  
  if (!filesFound) {
    console.log('\nKeine Dateien im S3-Bucket gefunden. Der Bucket ist möglicherweise leer oder es gibt Berechtigungsprobleme.');
    console.log('Prüfen Sie, ob die AWS-Anmeldeinformationen korrekt konfiguriert sind und über ausreichende Berechtigungen verfügen.');
  }
}

// Script ausführen
main().catch(error => {
  console.error(`Unbehandelter Fehler: ${error.message}`);
  process.exit(1);
});
