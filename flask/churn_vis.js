/*
 * ****************************************************************************
 *
 *  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
 *  (C) Cloudera, Inc. 2021
 *  All rights reserved.
 *
 *  Applicable Open Source License: Apache 2.0
 *
 *  NOTE: Cloudera open source products are modular software products
 *  made up of hundreds of individual components, each of which was
 *  individually copyrighted.  Each Cloudera open source product is a
 *  collective work under U.S. Copyright Law. Your license to use the
 *  collective work is as provided in your written agreement with
 *  Cloudera.  Used apart from the collective work, this file is
 *  licensed for your use pursuant to the open source license
 *  identified above.
 *
 *  This code is provided to you pursuant a written agreement with
 *  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
 *  this code. If you do not have a written agreement with Cloudera nor
 *  with an authorized and properly licensed third party, you do not
 *  have any rights to access nor to use this code.
 *
 *  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
 *  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
 *  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
 *  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
 *  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
 *  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
 *  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
 *  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
 *  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
 *  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
 *  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
 *  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
 *  DATA.
 *
 * ***************************************************************************
 */

//This is the javascript code that builds and updates the bar graph

window.updater = function(data) {
  //d3.select("#svg_container").text(data);
  my_data = data;
  console.log(data);

//    var svg_margin = { top: 20, right: 20, bottom: 20, left: 40 };
//    var svg_width = d3.select("body").node().getBoundingClientRect().width - svg_margin.left - svg_margin.right;
//    var svg_height = 300 - svg_margin.top - svg_margin.bottom;
//
//    var y = d3.scaleLinear()
//        .domain([0, d3.max(data, function(d) { return d.petal_length; })])
//        .range([svg_height, 0]);
//
//    var x = d3.scaleBand()
//        .domain(d3.range(data.length))
//        .range([0, svg_width])
//        .padding(0.1);
//
//    var species_list = d3.map(data, function (d) { return d.species;}).keys();
//
//    if (d3.select("#svg_container").select("svg").empty()) {
//
//
//        svg = d3.select("#svg_container").append("svg")
//          .attr("width", svg_width + svg_margin.left + svg_margin.right)
//          .attr("height", svg_height + svg_margin.top + svg_margin.bottom)
//          .append("g")
//          .attr("transform",
//              "translate(" + svg_margin.left + "," + svg_margin.top + ")");
//
//        svg.append("g")
//            .attr("transform", "translate(0," + svg_height + ")")
//            .attr("class", "x axis")
//            .call(d3.axisBottom(x));
//
//        // add the y Axis
//        svg.append("g")
//            .attr("class", "y axis")
//            .call(d3.axisLeft(y));
//    } else {
//        svg.attr("width", svg_width + svg_margin.left + svg_margin.right)
//        svg.selectAll("g.y.axis")
//            .call(d3.axisLeft(y));
//
//        svg.selectAll("g.x.axis")
//            .call(d3.axisBottom(x));
//    }
//
//    // DATA JOIN
//    // Join new data with old elements, if any.
//
//    var bars = svg.selectAll(".bar")
//        .data(data);
//
//    // UPDATE
//    // Update old elements as needed.
//
//    bars
//        .attr("style",function(d) { return "fill:" + d3.schemeCategory10[species_list.indexOf(d.species)];})
//        .attr("x", function(d, i) { return x(i); })
//        .attr("width", x.bandwidth())
//        .transition()
//        .duration(100)
//        .attr("y", function(d) { return y(d.petal_length); })
//        .attr("height", function(d) { return svg_height - y(d.petal_length); });
//
//    // ENTER + UPDATE
//    // After merging the entered elements with the update selection,
//    // apply operations to both.
//
//    bars.enter().append("rect")
//        .attr("class", "bar")
//        .attr("style",function(d) { return "fill:" + d3.schemeCategory10[species_list.indexOf(d.species)];})
//        .attr("x", function(d, i) { return x(i); })
//        .attr("width", x.bandwidth())
//        .attr("y", function(d) { return y(d.petal_length); })
//        .attr("height", function(d) { return svg_height - y(d.petal_length); })
//        .merge(bars);
//
//    // EXIT
//    // Remove old elements as needed.
//
//    bars.exit().remove();

};