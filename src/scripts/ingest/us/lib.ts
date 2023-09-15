import path from "path";
import _ from "lodash";
import { NameYear } from "@/types/NameYear";
import crypto from "crypto";

export const nullNameYear = (year: number): NameYear => ({
  y: year,
  t: 0,
  m: 0,
  f: 0,
  mr: null,
  fr: null,
  mp: 0,
  fp: 0,
});

type NameYearsByName = {
  [name: string]: undefined | Partial<NameYear & { y: number }>[];
};

export const handleRecords = (
  sortedRecords: {
    year: number;
    sex: "M" | "F";
    count: number;
    name: string;
  }[],
  startYear: number,
  endYear: number
): NameYearsByName => {
  const byName: NameYearsByName = {};
  const byYear = _.groupBy(sortedRecords, (r) => r.year);
  const totalsByYear: { [year: string]: { m: number; f: number } } = {};
  for (const [year, nameYears] of Object.entries(byYear)) {
    totalsByYear[year] = {
      m: nameYears.reduce(
        (sum, ny) => (ny.sex === "M" ? sum + ny.count : sum),
        0
      ),
      f: nameYears.reduce(
        (sum, ny) => (ny.sex === "F" ? sum + ny.count : sum),
        0
      ),
    };
  }

  let rankCounter = 0;
  let currentYear = startYear;
  for (const r of sortedRecords) {
    if (r.year !== currentYear) {
      rankCounter = 1;
      currentYear = r.year;
    } else {
      rankCounter += 1;
    }
    // backfill nameyears
    if (!byName[r.name]) {
      // First time encountering this name.
      byName[r.name] = [];
    }

    if (r.sex === "F") {
      byName[r.name]!.push({
        ...nullNameYear(currentYear),
        t: r.count,
        f: r.count,
        fr: rankCounter,
        fp: r.count / totalsByYear[r.year].f,
      });
    }
    if (r.sex === "M") {
      const existingInd = byName[r.name]!.findIndex((ny) => ny.y === r.year);
      if (existingInd === -1) {
        byName[r.name]!.push({
          ...nullNameYear(r.year),
          t: r.count,
          m: r.count,
          mr: rankCounter,
          mp: r.count / totalsByYear[r.year].m,
        });
      } else {
        byName[r.name]![existingInd] = {
          ...byName[r.name]![existingInd],
          m: r.count,
          mr: rankCounter,
          mp: r.count / totalsByYear[r.year].m,
          t: (byName[r.name]![existingInd].f || 0) + r.count,
        };
      }
    }
  }

  return byName;
};

const partition = (str: string, partitionCount: number = 300) => {
  const hash = crypto.createHash("md5").update(str).digest("hex");
  const largeNumber = BigInt(`0x${hash}`);
  return `p${_.padStart(
    `${Number(largeNumber % BigInt(partitionCount))}`,
    3,
    "0"
  )}`;
};

export const seriesPath = (name: string, region: string) => {
  return `data/processed/names/${partition(
    name.toLowerCase()
  )}/${name.toLowerCase()}/series/${region
    .toLowerCase()
    .split("-")
    .join("/")}/`;
};

export const seriesOutputDir = (name: string, region: string) => {
  return path.join(
    __dirname,
    `../../../..`,
    seriesPath(name.toLowerCase(), region)
  );
};

export const seriesOutputPath = (name: string, region: string) => {
  return `${seriesOutputDir(name.toLowerCase(), region)}series.json`;
};
