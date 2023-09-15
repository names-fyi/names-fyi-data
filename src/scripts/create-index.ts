import { IndexEntry } from "@/types/IndexEntry";
import { NameYear } from "@/types/NameYear";
import fs from "fs";
import _, { min } from "lodash";
import path from "path";
import { seriesOutputPath } from "./ingest/us/lib";

const createCsv = (index: IndexEntry[]) => {
  return index.map((i) => `${i.n},${i.r}`).join("\n");
};
const run = async () => {
  const partitionDir = path.join(__dirname, "../../data/processed/names");
  const partitions = await fs.promises.readdir(partitionDir);
  const allNames: string[] = [];
  for (const p of partitions) {
    allNames.push(...(await fs.promises.readdir(`${partitionDir}/${p}`)));
  }
  const index: IndexEntry[] = [];
  const chunks = _.chunk(allNames, 20);
  console.log(`running on ${allNames.length} names in ${chunks.length} chunks`);
  for (const chunk of chunks) {
    await Promise.all(
      chunk.map(async (n) => {
        let series: NameYear[];
        try {
          series = JSON.parse(
            (
              await fs.promises.readFile(seriesOutputPath(n, "us-national"))
            ).toString()
          );
        } catch (err) {
          index.push({ n, r: null });
          return;
        }
        let minRank: number | null =
          min(
            series.map((ny) => Math.min(ny.mr || Infinity, ny.fr || Infinity))
          ) || null;
        if (minRank === Infinity) minRank = null;
        index.push({ n, r: minRank });
      })
    );
  }
  console.log("sorting and writing");
  const outDir = path.join(__dirname, "../../data/processed");
  const sortedInd = _.sortBy(index, (i) => i.n);
  await fs.promises.writeFile(
    `${outDir}/name-index.json`,
    JSON.stringify(sortedInd)
  );
  await fs.promises.writeFile(`${outDir}/name-index.csv`, createCsv(sortedInd));
  await fs.promises.writeFile(
    `${outDir}/name-index-1l.csv`,
    createCsv(_.sortBy(index, [(i) => i.n.slice(0, 1), (i) => i.r]))
  );
  await fs.promises.writeFile(
    `${outDir}/name-index-2l.csv`,
    createCsv(_.sortBy(index, [(i) => i.n.slice(0, 2), (i) => i.r]))
  );
  await fs.promises.writeFile(
    `${outDir}/name-index-3l.csv`,
    createCsv(_.sortBy(index, [(i) => i.n.slice(0, 3), (i) => i.r]))
  );
};

run()
  .then(() => "done")
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
