import csv from "csv-parse";
import { finished } from "stream/promises";
import fs from "fs";
import path from "path";
import _ from "lodash";
import { handleRecords, seriesOutputDir, seriesOutputPath } from "./lib";

type Record = {
  name: string;
  sex: "M" | "F";
  count: number;
};

const isRecord = (o: any): o is Record => {
  if (!_.isObject(o)) return false;
  if (!("name" in o) || !_.isString(o["name"])) return false;
  if (!("sex" in o) || !(o["sex"] === "M" || o["sex"] === "F")) return false;
  if (!("count" in o) || !_.isNumber(o["count"])) return false;
  return true;
};

const parseFile = async (filePath: string) => {
  const records: Record[] = [];
  const parser = fs.createReadStream(filePath).pipe(
    csv.parse({
      // CSV options if any
      columns: ["name", "sex", "count"],
      cast: true,
    })
  );
  parser.on("readable", function () {
    let record;
    while ((record = parser.read()) !== null) {
      // Work with each record
      if (!isRecord(record)) {
        throw new Error(`Invalid record: ${JSON.stringify(record)}`);
      }
      records.push(record);
    }
  });
  await finished(parser);
  return records;
};

const MIN_YEAR = 1880;
const YEAR = 2022;
const NUM = undefined;
const yearFileRegex = /yob(\d{4}).txt/;

const run = async () => {
  const inputRoot = path.join(
    __dirname,
    `../../../../data/raw/2022/us/national/`
  );

  let inputFiles = fs
    .readdirSync(inputRoot)
    .filter((f) => yearFileRegex.test(f));

  if (NUM !== undefined && NUM !== null) {
    inputFiles = inputFiles.slice(0, NUM);
  }

  let allRecords = [];
  for (const f of inputFiles) {
    const records = await parseFile(`${inputRoot}${f}`);
    const year = parseInt(f.match(yearFileRegex)![1]!, 10);
    allRecords.push(...records.map((r) => ({ ...r, year, state: "ALL" })));
  }
  allRecords = _.sortBy(allRecords, ["sex", "year", (r) => -r.count]);

  const byName = handleRecords(allRecords, MIN_YEAR, YEAR);
  const batches = _.chunk(Object.entries(byName), 10);
  for (const batch of batches) {
    await Promise.all(
      batch.map(async ([name, nameYears]) => {
        await fs.promises.mkdir(seriesOutputDir(name, "us-national"), {
          recursive: true,
        });
        await fs.promises.writeFile(
          seriesOutputPath(name, "us-national"),
          JSON.stringify(_.sortBy(nameYears, (ny) => ny.y))
        );
      })
    );
  }
};

run()
  .then(() => "done")
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
