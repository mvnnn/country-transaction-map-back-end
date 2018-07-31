import express from "express";
import http from "http";
import path from "path";
import cookieParser from "cookie-parser";
import logger from "morgan";
import bodyParser from "body-parser";
import cors from "cors";
import expressValidator from "express-validator";
import methodOverride from "method-override";

import csv from "fast-csv";
import fs from "fs";
import util from "util";
import stream from "stream";
import es from "event-stream";

import Project from "./routes/project";

const port = process.env.PORT || 4000;
const app = express();

app.use(
  cors({
    exposedHeaders: ["Link"]
  })
);

app.use(
  bodyParser.json({
    limit: "100kb"
  })
);

app.use(logger("dev"));
app.use(cookieParser());
app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true
  })
);

app.get("/transaction/:country_code/:activePage", Project.GetAllIpTransaction);
app.get("/countrytransaction/:country_code", Project.GetAllCountryTransaction);
app.get("/generateCSV", Project.GenerateCSV);

// Get all country details and send it in response
app.get("/country", function(req, res) {
  let result = [];
  var stream = fs
    .createReadStream(path.resolve("./assets", "countries.csv"))
    .pipe(csv.parse({ headers: true }))
    .transform(function(row, next) {
      next(null, {
        country_name: row.country_name,
        longitude: row.longitude,
        latitude: row.latitude,
        country_code: row.country_code
      });
    })
    .on("readable", function() {
      var row;
      while (null !== (row = stream.read())) {
        result.push(row);
        // console.log(row);
      }
    })
    .on("end", function() {
      res.send(result);
    });
});

app.use((req, res, next) => {
  let err = new Error("Not Found");
  err.status = 404;
  next(err);
});

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  next();
});

// Express validator
app.use(
  expressValidator({
    errorFormatter: function(param, msg, value) {
      var namespace = param.split("."),
        root = namespace.shift(),
        formParam = root;

      while (namespace.length) {
        formParam += "[" + namespace.shift() + "]";
      }
      return {
        param: formParam,
        msg: msg,
        value: value
      };
    }
  })
);

// error handler
// no stacktraces leaked to user unless in development environment
app.use((err, req, res, next) => {
  res.status(err.status || 500);
  res.json({
    response: "error",
    message: err.message,
    data: app.get("env") === "development" ? err : {}
  });
});

var server = http.createServer(app);
server.listen(4000, function() {
  console.log("Server is listening on port 4000");
});

export default app;
