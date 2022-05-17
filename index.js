const { promises: fs, existsSync, mkdirSync, copyFileSync } = require("fs");
const { promisify } = require("util");
const path = require('path');
const sqlite3 = require("sqlite3").verbose();
const mm = require("music-metadata");
const { exec } = require("child_process");
const sanitize = require("sanitize-filename");

const yargs = require('yargs');

const argv = yargs
      .option('outputdir', {
        alias: 'o',
        description: 'Base output directory',
        type: 'string',
        default: `${process.env.HOME}/Downloads/PodcastsExport`
      })
      .option('datesubdir', {
        alias: 'd',
        description: 'Add YYYY.MM.DD subdirectory to output dir',
        type: 'boolean',
        default: true
      })
      .option('pattern', {
        alias: 'p',
        description: 'File substring patterns to match',
        type: 'string'
      })
      .option('updateutime', {
        alias: 'u',
        description: 'Update the utime of the downloaded files',
        type: 'boolean',
        default: false
      })
      .option('no-spaces', {
        description: 'Replace filename spaces with underscores',
        type: 'boolean',
        default: false
      })
      .help()
      .alias('help', 'h').argv;


// Added the Podcast name to the query
// Looks like the date stored in the SQLite has an offset of +31 years, so we adjust the query
const podcastSelectSQL = `
  SELECT PC.ztitle as zpodcast, EP.zcleanedtitle as zcleanedtitle, EP.zuuid as zuuid,
    datetime(EP.zpubdate,'unixepoch','+31 years') date
    FROM ZMTPODCAST PC LEFT OUTER JOIN ZMTEPISODE EP
    ON PC.Z_PK = EP.ZPODCAST
`;
const fileNameMaxLength = 50;

function getOutputDirPath() {
  let ret = argv.outputdir;
  if (argv.datesubdir) {
    const d = new Date();
    const pad = (s) => s.toString().padStart(2, "0");
    const month = pad(d.getMonth() + 1);
    const day = pad(d.getDate());
    const currentDateFolder = `${d.getFullYear()}.${month}.${day}`;
    ret = `${ret}/${currentDateFolder}`;
  }
  return ret;
}

async function getPodcastsBasePath() {
  const groupContainersFolder = `${process.env.HOME}/Library/Group Containers`;
  try {
    const libraryGroupContainersDirList = await fs.readdir(
      groupContainersFolder
    );
    const podcastsAppFolder = libraryGroupContainersDirList.find((d) => d.includes("groups.com.apple.podcasts"));
    if (!podcastsAppFolder) {
      throw new Error(
        `Could not find podcasts app folder in ${groupContainersFolder}`
      );
    }
    return `${process.env.HOME}/Library/Group Containers/${podcastsAppFolder}`;
  } catch (e) {
    throw new Error(
      `Could not find podcasis app folder in ${groupContainersFolder}, original error: ${e}`
    );
  }
}

async function getPodcastsDBPath() {
  return `${await getPodcastsBasePath()}/Documents/MTLibrary.sqlite`;
}

async function getPodcastsCacheFilesPath() {
  return `${await getPodcastsBasePath()}/Library/Cache`;
}

async function getDBPodcastsData() {
  const dbOrigin = new sqlite3.Database(await getPodcastsDBPath());
  const db = {
    serialize: promisify(dbOrigin.serialize).bind(dbOrigin),
    all: promisify(dbOrigin.all).bind(dbOrigin),
    close: promisify(dbOrigin.close).bind(dbOrigin)
  };

  try {
    await db.serialize();
    return await db.all(podcastSelectSQL);
  } finally {
    try {
      db.close();
    } catch (e) {
      console.error(e);
    }
  }
}

async function tryGetDBPodcastsData() {
  try {
    return await getDBPodcastsData();
  } catch (error) {
    console.error("Could not fetch data from podcasts database:", error);
    return [];
  }
}

async function getMP3MetaTitle(path) {
  const mp3Metadata = await mm.parseFile(path);
  return mp3Metadata?.common?.title;
}

async function getPodcastsCacheMP3Files(cacheFilesPath) {
  try {
    const podcastFiles = await fs.readdir(cacheFilesPath);
    return podcastFiles.filter((f) => f.includes(".mp3"));
  } catch (e) {
    throw new Error(`Could not find mp3 files in podcasts cache folder either there are no downloaded podcasts or something changed in podcasts app
original error: ${e}`);
  }
}


function handleSpaces(s) {
  ret = s;
  if (argv.nospaces) {
    ret = s.replaceAll(' ', '_');
  }
  return ret;
}


async function buildPodcastDict(fileName, cacheFilesPath, podcastsDBData) {
  const uuid = fileName.replace(".mp3", "");
  const dbMeta = podcastsDBData.find((m) => m.zuuid === uuid);
  const path = `${cacheFilesPath}/${fileName}`;
  const exportBase = dbMeta?.zcleanedtitle // 1. from apple podcast database
        ?? (await getMP3MetaTitle(path)) // 2. from mp3 meta data
        ?? uuid; // 3. fallback to unreadable uuid
  const podcastName = dbMeta?.zpodcast.replaceAll('/', '_');
  const exportFileName = sanitize(exportBase.substr(0, fileNameMaxLength));

  const ret = {
    podcastName: handleSpaces(podcastName),
    date: dbMeta?.date,
    fileName,
    path,
    uuid,
    exportFileName: handleSpaces(`${exportFileName}.mp3`)
  };
  return ret;
}


function filterPodcasts(podcasts, filepatterns = []) {
  if (filepatterns.length == 0) {
    return podcasts;
  }

  function matchesAny(s) {
    return filepatterns.some((p) => { return s.indexOf(p) != -1 })
  }

  return podcasts.filter((p) => {
    return matchesAny(p.exportFileName) || matchesAny(p.podcastName);
  });
}


async function exportSingle(podcast) {
  copyFileSync(podcast.path, podcast.newPath);
  if (podcast.date && argv.updateutime) {
    const d = new Date(podcast.date);
    await fs.utimes(newPath, d, d);
  }
}


async function getPodcastsToExport(podcastsDBData, filepatterns = []) {
  const cacheFilesPath = await getPodcastsCacheFilesPath();
  const podcastMP3Files = await getPodcastsCacheMP3Files(cacheFilesPath);
  const podcasts = await Promise.all(podcastMP3Files.map((fileName) => {
    return buildPodcastDict(fileName, cacheFilesPath, podcastsDBData);
  }));
  let filteredPodcasts = filterPodcasts(podcasts, filepatterns);

  // Weirdly, there are some podcasts that are duplicates ...  i.e.,
  // if you uncomment the below code, it prints some duplicate names,
  // in my case at least.
  /*
    const allNames = filteredPodcasts.map(p => p.exportFileName);
    console.log(`have ${allNames.length} names`);
    const uniqueNames = Array.from(new Set(allNames));
    console.log(`have ${uniqueNames.length} names`);
    const dups = allNames.filter((e, i, a) => a.indexOf(e) !== i);
    console.log(`duplicates! ${dups}`);
    process.exit(0);
  */
  // Since this causes some strange messages to appear during output
  // to a new output directory, delete the dups by keying on filename.
  const temp = {}
  // console.log('removing dups');
  filteredPodcasts.forEach(p => temp[p.exportFileName] = p);
  filteredPodcasts = Object.values(temp);
  // console.log('done dups');

  return filteredPodcasts;
}


function addFilePaths(p, outputDir) {
  function joinPath(parts) {
    return parts.filter((s) => s).join('/');
  }

  const parts = [outputDir, p.podcastName, p.exportFileName];
  p.newPath = joinPath(parts);
  p.logName = joinPath([p.podcastName, p.exportFileName]);
}


async function exportPodcasts(podcastsDBData, filepatterns = []) {
  const filteredPodcasts = await getPodcastsToExport(podcastsDBData, filepatterns);
  if (filepatterns.length > 0) {
    console.log(`Exporting ${filteredPodcasts.length} podcasts.`);
  }
  else {
    console.log('No podcasts to export, quitting.');
    return;
  }

  const outputDir = getOutputDirPath();
  filteredPodcasts.forEach(p => addFilePaths(p, outputDir));

  // Make all necessary directories, directory per podcast.
  const allDirs = filteredPodcasts.map(p => path.dirname(p.newPath));
  const uniqueDirs = Array.from(new Set(allDirs));
  uniqueDirs.forEach(d => mkdirSync(d, { recursive: true }));

  let skipped = 0;

  // Actual file export.
  await Promise.all(
    filteredPodcasts.map(async (p) => {
      if (!existsSync(p.newPath)) {
        console.log(p.logName);
        await exportSingle(p);
      }
      else {
        skipped += 1;  // Might not work w/ promises, but not concerned.
        // console.log(`Already have ${logName}, skipping`);
      }
    })
  );

  console.log(`\n\nExported ${filteredPodcasts.length} podcasts to '${outputDir}'`);
  if (skipped > 0) {
    console.log(`(skipped ${skipped}, already present)`);
  }
  exec(`open ${outputDir}`);
}

async function main(filepatterns = []) {
  const dbPodcastData = await tryGetDBPodcastsData();
  await exportPodcasts(dbPodcastData, filepatterns);
}

// Default: return all files.
let patterns = []
if (argv.pattern) {
  // User might specify one pattern, in which case argv.pattern is a
  // string, or multiple, in which case it's an array.
  patterns = [ argv.pattern ].flat();
}

main(patterns);
