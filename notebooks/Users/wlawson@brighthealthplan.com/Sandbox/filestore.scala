// Databricks notebook source
// MAGIC %md # **FileStore**
// MAGIC The FileStore is a special folder within DBFS where you can save files and have them accessible in your web browser.
// MAGIC * Use the FileStore to save files that are accessible within HTML and JavaScript when you call `DisplayHTML`.
// MAGIC * Use the FileStore to save output files that you want to download to your local desktop.

// COMMAND ----------

// MAGIC %md ### Download a new JavaScript library that you wish to use in your `displayHTML()` code to the local disk.

// COMMAND ----------

import sys.process._

// COMMAND ----------

"sudo apt-get -y install wget" !!

// COMMAND ----------

"wget -P /tmp http://d3js.org/d3.v3.min.js" !!

// COMMAND ----------

// MAGIC %md ### List `file:/tmp` to verify that the file has been downloaded to the local disk

// COMMAND ----------

display(dbutils.fs.ls("file:/tmp/d3.v3.min.js"))

// COMMAND ----------

display(dbutils.fs.ls("file:/tmp"))

// COMMAND ----------

// MAGIC %md ### Copy the downloaded file from `/tmp` into `/FileStore/customjs` to be used by your HTML

// COMMAND ----------

// dbutils.fs.mkdirs("/FileStore/customjs")
// dbutils.fs.cp("file:/tmp/d3.v3.min.js", "/FileStore/customjs/d3.v3.min.js")

// COMMAND ----------

// MAGIC %md ### List the `/FileStore/customjs` directory to verify that the file has been copied

// COMMAND ----------

display(dbutils.fs.ls("/FileStore/customjs"))

// COMMAND ----------

// MAGIC %md ### Reference the JavaScript library in a script run in the browser
// MAGIC From the browser, the library is in the `/files` directory: `/files/customjs/d3.v3.min.js`

// COMMAND ----------

displayHTML(s"""
<!DOCTYPE html>
<meta charset="utf-8">
<body>
<script src="/files/customjs/d3.v3.min.js"></script>
<script>
var width = 200
var height = 200
var vertices = d3.range(100).map(function(d) {
  return [Math.random() * width, Math.random() * height];
});

var lineFunction = d3.svg.line()
                         .x(function(d) { return d[0]; })
                         .y(function(d) { return d[1]; })
                         .interpolate("linear");

//The SVG Container
var svgContainer = d3.select("body").append("svg")
                                    .attr("width", 200)
                                    .attr("height", 200);

//The line SVG Path we draw
var lineGraph = svgContainer.append("path")
                            .attr("d", lineFunction(vertices))
                            .attr("stroke", "blue")
                            .attr("stroke-width", 2)
                            .attr("fill", "none");
</script>
  """)