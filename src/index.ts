import { createConnection, Connection } from "mysql2/promise";
import fs from "fs";
import readline from "readline";
import extract from "extract-zip";

import { downloadFile } from "./utils";

(async () => {
  // put here pair 
  let pair = "BTCBUSD"; 
  // put here date for starting parsing YY.MM.DD
  let unix_begin = Math.floor(new Date("2022.01.01").getTime() / 1000); 
  // put here date for end parsing YY.MM.DD
  let unix_end = Math.floor(new Date("2022.03.01").getTime() / 1000);
  // put here your database
    const connection = await createConnection({
      host: "localhost",
      user: "root",
      database: "binance_parse",
      password: "root",
      port: 3306,
    });
    await connection.query("SELECT 1+1;");
    const insert_trades = async (arr: any) => {
      const sql: string = "INSERT IGNORE INTO `binance_trades` (trade_id, price, qty, quote_qty, time, is_buyer_maker, is_best_match, coin_id) VALUES ?";
      const result = await connection.query(sql, [arr]);
      
      return result;
    };
  for (let i = unix_begin; i <= unix_end; i += 2716144) {
      let arr: any[] = [];
      const date = new Date(i * 1000);
      let monthStr = String(date.getMonth() + 1);
      const yearStr = String(date.getFullYear());
      monthStr = ("0" + monthStr).slice(-2);
      console.log(monthStr, yearStr);
      const file_path = `./files/${i}.zip`;
      await downloadFile(`https://data.binance.vision/data/spot/monthly/trades/${pair}/${pair}-trades-${yearStr}-${monthStr}.zip`, file_path);
      await extract(`C:\\Users\\zamazz\\Desktop\\projects\\pivo\\files\\${i}.zip`, { dir: "C:\\Users\\zamazz\\Desktop\\projects\\pivo\\files" });
      const fileStream = fs.createReadStream(`./files/BTCBUSD-trades-${yearStr}-${monthStr}.csv`);
      const rl = readline.createInterface({ input: fileStream, });
      const coin_id = 2;
      for await (const line of rl) {
        let parse = line.split(",");
        arr.push(parse);
        if (arr.length > 1024 ) {
          let local = [...arr];
          arr = [];
          let result = local.map((item: string) => {
            if (isNaN(Number(item[1]))) return undefined;
              return [
                Number(item[0]) || 0,
                Number(item[1]) || 0,
                Number(item[2]) || 0,
                Number(item[3]) || 0,
                Number(item[4]) || 0,
                item[5] === "True",
                item[6] === "True",
                coin_id
            ];
          }).filter((value) => value != undefined);
          await insert_trades(result);
      }
    }
    
    let result = arr.map((item: string) => {
      if (isNaN(Number(item[1]))) return undefined;
            return [
              Number(item[0]) || 0,
              Number(item[1]) || 0,
              Number(item[2]) || 0,
              Number(item[3]) || 0,
              Number(item[4]) || 0,
              item[5] === "True",
              item[6] === "True",
              coin_id
            ];
          }).filter((value) => value != undefined);
          await insert_trades(result);
  }
})();
