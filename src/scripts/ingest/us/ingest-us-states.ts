import csv from "csv-parse";
import fs from "fs";
import path from "path";
import _ from "lodash";
import { handleRecords, seriesOutputDir, seriesOutputPath } from "./lib";
import { finished } from "stream/promises";

const MIN_YEAR = 1910;
const YEAR = 2022;
const NUM = undefined;
const stateFileRegex = /STATE\.([A-Z]{2})\.TXT/;

export type Record = {
  state: string;
  sex: "M" | "F";
  year: number;
  name: string;
  count: number;
};
export const isRecord = (o: any): o is Record => {
  if (!_.isObject(o)) return false;
  if (!("state" in o) || !_.isString(o["state"])) return false;
  if (!("sex" in o) || !(o["sex"] === "M" || o["sex"] === "F")) return false;
  if (!("year" in o) || !_.isNumber(o["year"])) return false;
  if (!("name" in o) || !_.isString(o["name"])) return false;
  if (!("count" in o) || !_.isNumber(o["count"])) return false;
  return true;
};

const parseFile = async (filePath: string) => {
  const records: Record[] = [];
  const parser = fs.createReadStream(filePath).pipe(
    csv.parse({
      columns: ["state", "sex", "year", "name", "count"],
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

const run = async () => {
  const inputRoot = path.join(
    __dirname,
    `../../../../data/raw/${YEAR}/us/namesbystate/`
  );

  let inputFiles = fs
    .readdirSync(inputRoot)
    .filter((f) => stateFileRegex.test(f));

  if (NUM !== undefined && NUM !== null) {
    inputFiles = inputFiles.slice(0, NUM);
  }

  for (const f of inputFiles) {
    const records = await parseFile(`${inputRoot}${f}`);
    const stateId = f.match(stateFileRegex)?.[1]!;
    const byName = handleRecords(records, MIN_YEAR, YEAR);
    for (const [name, nameYears] of Object.entries(byName)) {
      await fs.promises.mkdir(seriesOutputDir(name, `us-${stateId}`), {
        recursive: true,
      });
      fs.writeFileSync(
        seriesOutputPath(name, `us-${stateId}`),
        JSON.stringify(_.sortBy(nameYears, (ny) => ny.y))
      );
    }
  }
};

run()
  .then(() => "done")
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
