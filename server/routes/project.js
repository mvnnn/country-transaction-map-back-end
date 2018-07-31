import fs from "fs";
import es from "event-stream";
import path from "path";

// filter country ip transaction data
function filterIp(country_code) {
  var rows = [];
  return new Promise((res, rej) => {
    var s = fs
      .createReadStream(path.resolve("./assets", "ip_to_multiple.csv"))
      .pipe(es.split())
      .pipe(
        es
          .mapSync(function(line) {
            // pause the readstream
            s.pause();

            let [
              ip_src,
              ip_dst,
              country_src,
              country_dest,
              timestamp,
              totalInteraction
            ] = line.split(",");

            if (country_code === country_src) {
              rows.push({
                ip_src: ip_src,
                ip_dst: ip_dst,
                country_dest: country_dest,
                timestamp: timestamp,
                totalInteraction: totalInteraction
              });
            }
            // process line here and call s.resume() when rdy
            // function below was for logging memory usage
            // logMemoryUsage(lineNr);

            // resume the readstream, possibly from a callback
            s.resume();
          })
          .on("error", function(err) {
            //   console.log('Error while reading file.', err);
            rej(err);
          })
          .on("end", function() {
            // console.log('Read entire file.', lineNr);
            res(rows);
          })
      );
  });
}

// filter country to country transaction data
function filterCountry(country_code) {
  var rows = [];
  return new Promise((res, rej) => {
    var s = fs
      .createReadStream(path.resolve("./assets", "src_to_dest.csv"))
      .pipe(es.split())
      .pipe(
        es
          .mapSync(function(line) {
            // pause the readstream
            s.pause();

            let [country_src, country_dest, total_transaction] = line.split(
              ","
            );

            if (country_code === country_src) {
              rows.push({
                country_dest: country_dest,
                total_transaction: total_transaction
              });
            }
            // process line here and call s.resume() when rdy
            // function below was for logging memory usage
            // logMemoryUsage(lineNr);

            // resume the readstream, possibly from a callback
            s.resume();
          })
          .on("error", function(err) {
            //   console.log('Error while reading file.', err);
            rej(err);
          })
          .on("end", function() {
            // console.log('Read entire file.', lineNr);
            res(rows);
          })
      );
  });
}

export default {
  // Get country ip transaction data and send it in response
  GetAllIpTransaction(req, res) {
    filterIp(req.params.country_code)
      .then(r => {
        let transaction_no = (req.params.activePage - 1) * 10;
        let transaction_last_no = req.params.activePage * 10;
        if (transaction_no + 10 <= r.length) {
          transaction_last_no = transaction_no + 10;
        } else {
          transaction_last_no = r.length;
        }

        let response = {
          no_of_transaction: Math.ceil(r.length / 10),
          top_10_transaction: r.slice(transaction_no, transaction_last_no)
        };
        res.send(response);
      })
      .catch(e => console.error(e));
  },

  // Get all no. of country to country transaction and send it in response
  GetAllCountryTransaction(req, res) {
    filterCountry(req.params.country_code)
      .then(r => {
        res.send(r);
      })
      .catch(e => console.error(e));
  },

  // Manipulate file data and generate 2 new files :
  // (1) ip_to_multiple : ip_src,ip_dst,country_src,country_dest,timestamp,totalInteraction
  // (2) src_to_dst : country_src,country_dest,total_transaction
  // It's takes only O(n) times
  GenerateCSV(req, res) {
    async function main() {
      try {
        var ip_state_rows = await readBigCSV("IP_state");
        var ip_to_country = new Map();
        ip_state_rows.forEach(r => {
          ip_to_country.set(r[0], r[1]);
        });
        await computeCountryToCountry(ip_to_country);
        await computeIpToMultiple(ip_to_country);
      } catch (e) {
        console.error(e);
      }
    }

    function readBigCSV(filename) {
      var rows = [];
      return new Promise((res, rej) => {
        var s = fs
          .createReadStream(path.resolve("./assets", filename + ".csv"))
          .pipe(es.split())
          .pipe(
            es
              .mapSync(function(line) {
                // pause the readstream
                s.pause();

                rows.push(line.split(","));

                // process line here and call s.resume() when rdy
                // function below was for logging memory usage
                // logMemoryUsage(lineNr);

                // resume the readstream, possibly from a callback
                s.resume();
              })
              .on("error", function(err) {
                //   console.log('Error while reading file.', err);
                rej(err);
              })
              .on("end", function() {
                // console.log('Read entire file.', lineNr);
                res(rows);
              })
          );
      });
    }

    async function computeCountryToCountry(ip_to_country) {
      var ip_communications_rows = await readBigCSV("IP_communications");
      var country_to_country = new Map();

      var wstream = fs.createWriteStream("src_to_dest.csv");
      for (var [src_ip, dest_ip] of ip_communications_rows) {
        var country_src = ip_to_country.get(src_ip);
        var country_dest = ip_to_country.get(dest_ip);
        if (!country_src) continue;
        var src_to_dest = country_src + "," + country_dest;
        var count = country_to_country.get(src_to_dest) || 0;
        count++;
        country_to_country.set(src_to_dest, count);
      }
      for (var [key, val] of country_to_country) {
        if (!val) continue;
        var [src, dest] = key.split(",");
        wstream.write(src + "," + dest + "," + val + "\n");
      }
      wstream.end(function() {
        console.log("done computeCountryToCountry");
      });
    }

    async function computeIpToMultiple(ip_to_country) {
      var ip_communications_rows = await readBigCSV("IP_communications");
      var ip_to_multiple = new Map();
      for (var [src_ip, dest_ip, timestamp] of ip_communications_rows) {
        var country_src = ip_to_country.get(src_ip);
        var country_dest = ip_to_country.get(dest_ip);

        var uid = src_ip + "," + dest_ip;

        var totalInteraction = 1;
        if (ip_to_multiple.has(uid)) {
          // increment the totalInteraction which is at position 4.
          totalInteraction = ip_to_multiple.get(uid)[4] + 1;
        }
        var finalRow = [
          src_ip,
          dest_ip,
          country_src,
          country_dest,
          timestamp,
          totalInteraction
        ];
        ip_to_multiple.set(uid, finalRow);
      }
      var wstream = fs.createWriteStream("ip_to_multiple.csv");
      for (var [key, val] of ip_to_multiple) {
        if (!val || val.length === 0) continue;
        // key would be uid, and val would be finalRow
        wstream.write(val.join(",") + "\n");
      }
      wstream.end(function() {
        console.log("done computeIpToMultiple");
      });
    }

    main();
  }
};
