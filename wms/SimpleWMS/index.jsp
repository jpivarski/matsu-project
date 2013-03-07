<html>
  <head>
    <meta http-equiv="content-type" content="text/html;charset=UTF-8" />
    <title>SimpleWMS</title>
    <style type="text/css">
      .spacer { margin-left: 10px; margin-right: 10px; }
      .layer_checkbox { margin-left: 20px; margin-top: 2px; margin-bottom: 2px; }
      table { table-layout: fixed; } 
      .header { font-weight: bold; vertical-align: bottom; }
      .row { max-width: 200px; overflow: hidden; padding-left: 5px; padding-right: 5px; }
      .even { background: #e7e7e7; }
      .odd { background: #f3f3f3; }
      .cell { text-align: left; }
      .clickableblue:hover { background: #c2d0f2; }
      .clickablered:hover { background: #fcb4ae; }
    </style>
    <script type="text/javascript" src="http://maps.googleapis.com/maps/api/js?key=AIzaSyAVNOfpLX6KdByplQxeMH1kuPZcYWBmz3c&sensor=false"></script>
    <link rel="stylesheet" href="css/black-tie/jquery-ui-1.8.23.custom.css">
    <script type="text/javascript" src="js/jquery-1.8.0.min.js"></script>
    <script type="text/javascript" src="js/jquery-ui-1.8.23.custom.min.js"></script>
    <script type="text/javascript">
// <![CDATA[

<%! 
public String giveMeSomething(String name, String defaultValue, HttpServletRequest request) {
    String x = request.getParameter(name);
    if (x == null) { return defaultValue; }
    else { return x; }
}   
%>

var map;
var circle;

var overlays = {};
var lat = <%= giveMeSomething("lat", "0.0", request) %>;
var lng = <%= giveMeSomething("lng", "0.0", request) %>;
var z = <%= giveMeSomething("z", "2", request) %>;

var rgb = <%= giveMeSomething("rgb", "true", request) %>;
var co2 = <%= giveMeSomething("co2", "false", request) %>;
var flood = <%= giveMeSomething("flood", "false", request) %>;
var TOPO_raw = <%= giveMeSomething("TOPO_raw", "false", request) %>;
var layers = [];
if (rgb) { layers.push("RGB"); }
if (co2) { layers.push("CO2"); }
if (flood) { layers.push("flood"); }
if (TOPO_raw) { layers.push("TOPO_raw"); }

var downloadsType = "l1g";
var uniqueFileNames = {};

var points = {};
var dontReloadPoints = {};
var oldsize;
var crossover = 4;
var showPoints = <%= giveMeSomething("showPoints", "true", request) %>;

var alldata;

var polygons = {};
var dontReloadPolygons = {};
var showPolygons = <%= giveMeSomething("showPolygons", "false", request) %>;
var olddepth;

var stats;
var stats_depth = -1;
var stats_numVisible = 0;
var stats_numInMemory = 0;
var stats_numPoints = 0;
var stats_numPolygons = 0;

var map_canvas;
var sidebar;

var minUserTime;
var maxUserTime;

var minScore = 0.0;
var maxScore = 10.0;
var minUserScore = 0.0;
var maxUserScore = 10.0;
var scoreField = "analyticscore";

var pointsArg = "<%= giveMeSomething("points", "none", request) %>";
var pointsTableName = "None";
if (pointsArg == "entities") {
    minScore = 0.0;
    maxScore = 10.0;
    minUserScore = 0.0;
    maxUserScore = 10.0;
    scoreField = "analyticscore";
    pointsTableName = "MatsuLevel2LngLat";
} else if (pointsArg == "clusters") {
    minScore = 0.0;
    maxScore = 30.0;
    minUserScore = 0.0;
    maxUserScore = 30.0;
    scoreField = "analyticsscore";
    pointsTableName = "MatsuLevel2Clusters";
}
else {
    minScore = 0.0;
    maxScore = 10.0;
    minUserScore = 5.0;
    maxUserScore = 10.0;
    scoreField = "analyticscore";
    pointsTableName = "None";
}

Number.prototype.pad = function(size) {
    if (typeof(size) !== "number") { size = 2; }
    var s = String(this);
    while (s.length < size) {
        s = "0" + s;
    }
    return s;
}

// Array.prototype.inclusiveRange = function(low, high) {
//     var i, j;
//     for (i = low, j = 0;  i <= high;  i++, j++) {
//         this[j] = i;
//     }
//     return this;
// }

Object.size = function(obj) {
    var size = 0;
    for (var key in obj) {
        if (obj.hasOwnProperty(key)) { size++; }
    }
    return size;
};

function isNumber(num) {
    return (typeof num == "string" || typeof num == "number") && !isNaN(num - 0) && num !== "";
};

function tileIndex(depth, longitude, latitude) {
    longitude += 180.0;
    latitude += 90.0;
    while (longitude <= 0.0) { longitude += 360.0; }
    while (longitude > 360.0) { longitude -= 360.0; }
    longitude = Math.floor(longitude/360.0 * Math.pow(2, depth+1));
    latitude = Math.min(Math.floor(latitude/180.0 * Math.pow(2, depth+1)), Math.pow(2, depth+1) - 1);
    return [depth, longitude, latitude];
}

function tileName(depth, longIndex, latIndex, layer) {
    return "T" + depth.pad(2) + "-" + longIndex.pad(5) + "-" + latIndex.pad(5) + "-" + layer;
}

function tileCorners(depth, longIndex, latIndex) {
    var longmin = longIndex*360.0/Math.pow(2, depth+1) - 180.0;
    var longmax = (longIndex + 1)*360.0/Math.pow(2, depth+1) - 180.0;
    var latmin = latIndex*180.0/Math.pow(2, depth+1) - 90.0;
    var latmax = (latIndex + 1)*180.0/Math.pow(2, depth+1) - 90.0;
    return new google.maps.LatLngBounds(
        new google.maps.LatLng(latmin, longmin),
        new google.maps.LatLng(latmax, longmax));
}

function doresize() {
    map_canvas.style.width = window.innerWidth - sidebar.offsetWidth - 20;
    var height = window.innerHeight - stats.offsetHeight - 20;
    map_canvas.style.height = height;
    sidebar.style.height = height - 10;
}

window.onresize = doresize;

function initialize() {
    var nodeList = document.querySelectorAll("input.layer-checkbox");
    for (var i in nodeList) {
	if (nodeList[i].type == "checkbox") {
            nodeList[i].checked = (layers.indexOf(nodeList[i].id.substring(6)) != -1);
	}
    }
    document.getElementById("show-points").checked = showPoints;

    if (pointsTableName == "None") {
        document.getElementById("points-pulldown").value = "None";
    }
    else if (pointsTableName == "MatsuLevel2LngLat") {
        document.getElementById("points-pulldown").value = "MatsuLevel2LngLat";
    }
    else if (pointsTableName == "MatsuLevel2Clusters") {
        document.getElementById("points-pulldown").value = "MatsuLevel2Clusters";
    }

    getTable();

    document.getElementById("show-polygons").checked = showPolygons;

    var latLng = new google.maps.LatLng(lat, lng);
    var options = {zoom: z, center: latLng, mapTypeId: google.maps.MapTypeId.TERRAIN};
    map = new google.maps.Map(document.getElementById("map_canvas"), options);
    google.maps.event.addListener(map, "bounds_changed", getEverything);

    circle = new google.maps.MarkerImage("circle.png", new google.maps.Size(18, 18), new google.maps.Point(0, 0), new google.maps.Point(9, 9), new google.maps.Size(18, 18));
    oldsize = 0;

    stats = document.getElementById("stats");
    map_canvas = document.getElementById("map_canvas");
    sidebar = document.getElementById("sidebar");
    doresize();
    sidebar.addEventListener("DOMAttrModified", doresize);

    minUserTime = 1325397600;    // Jan 1, 2012 00:00:00
    maxUserTime = 1357020000;    // Jan 1, 2013 00:00:00

    $(function() {
	$( "#slider-time" ).slider({
	    range: true,
	    min: minUserTime,
	    max: maxUserTime,
	    values: [minUserTime, maxUserTime],
	    slide: function( event, ui ) {
		minUserTime = ui.values[0];
		maxUserTime = ui.values[1];
	    },
	    stop: function( event, ui ) {
		for (var key in overlays) {
		    overlays[key].setMap(null);
		    delete overlays[key];
		}
		overlays = {};

		for (var key in points) {
		    points[key].setMap(null);
		    delete points[key];
		}
		points = {};
		dontReloadPoints = {};

                for (var key in polygons) {
                    polygons[key].setMap(null);
                    delete polygons[key];
                }
                polygons = {};
                dontReloadPolygons = {};
                setPolygonMetadata(null);

		getEverything();
	    }
	});
    });

    $(function() {
	$( "#slider-points" ).slider({
	    range: true,
	    min: 0.0,
	    max: 1.0,
            step: 0.01,
	    values: [(minUserScore - minScore)/(maxScore - minScore), (maxUserScore - minScore)/(maxScore - minScore)],
	    slide: function( event, ui ) {
		minUserScore = ui.values[0] * (maxScore - minScore) + minScore;
		maxUserScore = ui.values[1] * (maxScore - minScore) + minScore;
	    },
	    stop: function( event, ui ) {
                drawTable();

		for (var key in points) {
		    points[key].setMap(null);
		    delete points[key];
		}
		points = {};
		dontReloadPoints = {};

                if (showPoints) { getLngLatPoints(); }
                else { updateStatus(); }
	    }
	});
    });
}

function getTable() {
    if (pointsTableName == "None") {
        alldata = [];
        drawTable();
        return;
    }

    var xmlhttp = new XMLHttpRequest();
    xmlhttp.onreadystatechange = function() {
	if (xmlhttp.readyState == 4  &&  xmlhttp.status == 200) {
	    if (xmlhttp.responseText != "") {
		alldata = JSON.parse(xmlhttp.responseText)["data"];
		drawTable();
	    }
	}
    }
    xmlhttp.open("GET", "../TileServer/getTile?command=points&pointsTableName=" + pointsTableName, true);
    xmlhttp.send();
}

function drawTable(sortfield, numeric, increasing) {
    if (pointsTableName == "None") {
        document.getElementById("points-stuff").style.visibility = "hidden";
        document.getElementById("table-here").innerHTML = "";
        return;
    }

    document.getElementById("points-stuff").style.visibility = "visible";
    if (sortfield != null) {
	var inmetadata = (sortfield != "latitude"  &&  sortfield != "longitude"  &&  sortfield != "time");
	alldata.sort(function(a, b) {
            var aa, bb;
	    if (inmetadata) {
		aa = a["metadata"][sortfield];
	       	bb = b["metadata"][sortfield];
	    }
	    else {
		aa = a[sortfield];
		bb = b[sortfield];
	    }

	    if (numeric) {
		if (increasing) {
		    return aa - bb;
		}
		else {
		    return bb - aa;
		}
	    }
	    else {
		if (increasing) {
		    return aa > bb;
		}
		else {
		    return bb > aa;
		}
	    }
	});
    }

    var fields = ["latitude", "longitude", "acquisition time"];
    var nonNumericFields = [];
    var rowtexts = [];
    
    var ii = 0;
    for (var i in alldata) {
        if (alldata[i]["metadata"][scoreField] > minUserScore  &&  alldata[i]["metadata"][scoreField] < maxUserScore) {
	    var evenOdd = "even";
	    if (ii % 2 == 1) { evenOdd = "odd"; }
            ii++;

	    var row = alldata[i];
	    for (var m in row["metadata"]) {
                if (fields.indexOf(m) == -1) {
                    if (m != "analyticversion"  &&  m != "analytic"  &&  m != "image"  &&  m != "source"  &&  m != "version") {
		        fields.push(m);
                    }
	        }
	    }

	    var func = "map.setCenter(new google.maps.LatLng(" + row["latitude"] + ", " + row["longitude"] + ")); map.setZoom(13);";

	    var rowtext = "<tr id=\"table-" + row["identifier"] + "\" class=\"row " + evenOdd + " clickablered\" onmouseup=\"" + func + "\">";

	    for (var fi in fields) {
	        var f = fields[fi];
	        var s;
	        if (fi < 2) {
		    s = row[f];
	        }
	        else if (fi == 2) {
		    var d = new Date(1000 * row["time"]);
		    d.setMinutes(d.getMinutes() + d.getTimezoneOffset());  // get rid of any local timezone correction on the client's machine!
		    s = d.getFullYear() + "-" + (d.getMonth() + 1).pad(2) + "-" + d.getDate().pad(2) + " " + d.getHours().pad(2) + ":" + d.getMinutes().pad(2);
	        }
	        else {
		    s = row["metadata"][f];
	        }
	        rowtext += "<td class=\"cell\">" + s + "</td>";

	        if (fi != 2  &&  !isNumber(s)  &&  nonNumericFields.indexOf(f) == -1) {
		    nonNumericFields.push(f);
	        }
	    }
	    rowtext += "</tr>";

	    rowtexts.push(rowtext);
        }
    }

    var headerrow = "<tr class=\"row header\">";
    for (var fi in fields) {
	var f = fields[fi];
	var func = "drawTable('" + f + "', " + (nonNumericFields.indexOf(f) == -1) + ", " + (!increasing) + ");";
	headerrow += "<td class=\"cell clickableblue\" onmouseup=\"" + func + "\">" + f + "</td>";
    }
    headerrow += "</tr>\n";
    
    document.getElementById("table-here").innerHTML = "<table>\n" + headerrow + rowtexts.join("\n") + "\n</table>";
}

function getEverything() {
    getOverlays();
    if (showPolygons) { getPolygons(); }
    if (showPoints) { getLngLatPoints(); }
    else { updateStatus(); }
}

function setDownloadsType(index) {
    if (index == 0) {
        downloadsType = "l0";
    }
    else if (index == 1) {
        downloadsType = "l1g";
    }
    else if (index == 2) {
        downloadsType = "l1t";
    }
}

function toggleState(name, objname) {
    var obj = document.getElementById(objname);
    var newState = !(obj.checked);
    obj.checked = newState;

    var i = layers.indexOf(name);

    if (newState  &&  i == -1) {
	layers.push(name);
    }
    else if (!newState  &&  i != -1) {
	layers.splice(i, 1);

	for (var key in overlays) {
            if (key.substring(16) == name) {
		overlays[key].setMap(null);
		delete overlays[key];
	    }
	}
    }

    getOverlays();
}

function togglePoints(objname) {
    var obj = document.getElementById(objname);
    showPoints = obj.checked;
    if (showPoints) {
	getLngLatPoints();
    }
    else {
	for (var key in points) {
            points[key].setMap(null);
	    delete points[key];
	}
	points = {};
	dontReloadPoints = {};
	oldsize = -2;
	stats_numPoints = 0;
	updateStatus();
    }
}

function togglePolygons(objname) {
    var obj = document.getElementById(objname);
    showPolygons = obj.checked;
    if (showPolygons) {
        getPolygons();
    }
    else {
        for (var key in polygons) {
            polygons[key].setMap(null);
            delete polygons[key];
        }
        polygons = {};
        dontReloadPolygons = {};
        setPolygonMetadata(null);
        stats_numPolygons = 0;
        updateStatus();
    }
}

function getOverlays() {
    var bounds = map.getBounds();
    if (!bounds) { return; }

    var depth = map.getZoom() - 2;
    if (depth > 10) { depth = 10; }

    var longmin = bounds.getSouthWest().lng();
    var longmax = bounds.getNorthEast().lng();
    var latmin = bounds.getSouthWest().lat();
    var latmax = bounds.getNorthEast().lat();

    var tmp = tileIndex(depth, longmin, latmin);
    longmin = tmp[1];
    latmin = tmp[2];
    tmp = tileIndex(depth, longmax, latmax);
    longmax = tmp[1];
    latmax = tmp[2];

    var depthPad = depth.pad(2);
    for (var key in overlays) {
        if ((key[1] + key[2]) != depthPad) {
            overlays[key].setMap(null);
            delete overlays[key];
        }
    }

    stats_depth = depth;

    stats_numVisible = 0;
    var numAdded = 0;
    for (var i in layers) {
	for (var longIndex = longmin;  longIndex <= longmax;  longIndex++) {
            for (var latIndex = latmin;  latIndex <= latmax;  latIndex++) {
	    	var key = tileName(depth, longIndex, latIndex, layers[i]);
		if (!(key in overlays)) {
                    var overlayUrl = null;
                    if (layers[i] == "TOPO_raw") {
                        overlayUrl = "../TileServer/getTile?command=images&key=" + key;
                    }
                    else {
                        overlayUrl = "../TileServer/getTile?command=images&key=" + key + "&timemin=" + minUserTime + "&timemax=" + maxUserTime;
                    }
                    var overlay = new google.maps.GroundOverlay(overlayUrl, tileCorners(depth, longIndex, latIndex));
                    overlay.setMap(map);
                    overlays[key] = overlay;
                    numAdded++;
		}
		stats_numVisible++;
            }
	}
    }

    stats_numInMemory = 0;
    for (var key in overlays) {
        stats_numInMemory++;
    }
}

function updateDownloads() {
    var bounds = map.getBounds();
    if (!bounds) { return; }

    var depth = map.getZoom() - 2;
    if (depth > 10) { depth = 10; }

    var longmin = bounds.getSouthWest().lng();
    var longmax = bounds.getNorthEast().lng();
    var latmin = bounds.getSouthWest().lat();
    var latmax = bounds.getNorthEast().lat();

    var tmp = tileIndex(depth, longmin, latmin);
    longmin = tmp[1];
    latmin = tmp[2];
    tmp = tileIndex(depth, longmax, latmax);
    longmax = tmp[1];
    latmax = tmp[2];

    var filelist = document.getElementById("download-filelist");
    filelist.innerHTML = "(click button to update)";

    var udr = document.getElementById("download-udr");
    udr.innerHTML = "(click button to update)";

    var rsync = document.getElementById("download-rsync");
    rsync.innerHTML = "(click button to update)";

    uniqueFileNames = {};
    for (var longIndex = longmin;  longIndex <= longmax;  longIndex++) {
        for (var latIndex = latmin;  latIndex <= latmax;  latIndex++) {
            for (var layerIndex in layers) {
                var xmlhttp = new XMLHttpRequest();
                xmlhttp.onreadystatechange = function (XMLHTTP, FILELIST, UDR, RSYNC) { return function () {
                    if (XMLHTTP.readyState == 4  &&  XMLHTTP.status == 200) {
	                if (XMLHTTP.responseText != "") {
	                    var metadatas = JSON.parse(XMLHTTP.responseText);
	                    for (var i in metadatas) {
                                var metadata = prepareMetadata(metadatas[i]);

                                var bandName = null;
                                for (var key in metadata["L1T"]["PRODUCT_METADATA"]) {
                                    if (key.substr(0, 4) == "BAND"  &&  key.substr(-10) == "_FILE_NAME") {
                                        bandName = metadata["L1T"]["PRODUCT_METADATA"][key].substr(0, 22);
                                        break;
                                    }
                                }

                                if (bandName != null) {
                                    var year = bandName.substr(10, 4);
                                    var jday = bandName.substr(14, 3);
                                    var fileName = null;
                                    if (bandName.substr(0, 4) == "EO1H") {
                                        fileName = "/glusterfs/matsu/eo1/hyperion_" + downloadsType + "/" + year + "/" + jday + "/" + bandName + "_HYP_" + downloadsType.toUpperCase();
                                    }
                                    else {
                                        fileName = "/glusterfs/matsu/eo1/ali_" + downloadsType + "/" + year + "/" + jday + "/" + bandName + "_ALI_" + downloadsType.toUpperCase();
                                    }

                                    uniqueFileNames[fileName] = 1;
                                }
                            }

                            var command = [];
                            for (var fileName in uniqueFileNames) {
                                command.push(fileName);
                            }
                            if (command.length > 0) {
                                FILELIST.innerHTML = "udr rsync -av --stats --progress guest@opensciencedatacloud.org:{" + command.join(",") + "}";
                                UDR.innerHTML = "udr rsync -av --stats --progress guest@opensciencedatacloud.org:{" + command.join(",") + "} .";
                                RSYNC.innerHTML = "rsync -avzu guest@opensciencedatacloud.org:{" + command.join(",") + "} .";
                            }
                        }
                    }
                } }(xmlhttp, filelist, udr, rsync);
                
                var tileKey = tileName(depth, longIndex, latIndex, layers[layerIndex]);
                xmlhttp.open("GET", "../TileServer/getTile?command=imageMetadata&key=" + tileKey + "&timemin=" + minUserTime + "&timemax=" + maxUserTime, true);
                xmlhttp.send();
            }
        }
    }
}

function getLngLatPoints() {
    if (pointsTableName == "None") { return; }

    var bounds = map.getBounds();
    if (!bounds) { return; }

    var depth = map.getZoom() - 2;
    var size = 0;
    if (depth <= 9) {
	circle.size = new google.maps.Size(18, 18);
	circle.scaledSize = new google.maps.Size(18, 18);
	circle.anchor = new google.maps.Point(9, 9);
	if (depth <= crossover) {
            size = -1;
	}
    }
    else {
	size = Math.pow(2, depth - 10);
	circle.size = new google.maps.Size(36 * size, 36 * size);
	circle.scaledSize = new google.maps.Size(36 * size, 36 * size);
	circle.anchor = new google.maps.Point(18 * size, 18 * size);
    }

    if (oldsize != size) {
	for (var key in points) {
            points[key].setMap(null);
	    delete points[key];
	}
	points = {};
	dontReloadPoints = {};
    }
    oldsize = size;

    var longmin = bounds.getSouthWest().lng();
    var longmax = bounds.getNorthEast().lng();
    var latmin = bounds.getSouthWest().lat();
    var latmax = bounds.getNorthEast().lat();

    var tmp = tileIndex(10, longmin, latmin);
    longmin = tmp[1];
    latmin = tmp[2];
    tmp = tileIndex(10, longmax, latmax);
    longmax = tmp[1];
    latmax = tmp[2];

    var key = "" + depth + "-" + longmin + "-" + latmin + "-" + longmax + "-" + latmax;
    for (var oldkey in dontReloadPoints) {
	if (key == oldkey) {
	    return;
	}
    }
    dontReloadPoints[key] = true;

    var url;
    if (pointsTableName != "None") {
        if (size != -1) {
	    url = "../TileServer/getTile?command=points&longmin=" + longmin + "&longmax=" + longmax + "&latmin=" + latmin + "&latmax=" + latmax + "&pointsTableName=" + pointsTableName;
        }
        else {
	    url = "../TileServer/getTile?command=points&longmin=" + longmin + "&longmax=" + longmax + "&latmin=" + latmin + "&latmax=" + latmax + "&groupdepth=" + crossover + "&pointsTableName=" + pointsTableName;
        }
    }

    var xmlhttp = new XMLHttpRequest();
    xmlhttp.onreadystatechange = function() {
	if (xmlhttp.readyState == 4  &&  xmlhttp.status == 200) {
	    if (xmlhttp.responseText != "") {
		var data = JSON.parse(xmlhttp.responseText)["data"];
		for (var i in data) {
	    	    var identifier = data[i]["identifier"];
                    if (data[i]["metadata"][scoreField] > minUserScore  &&  data[i]["metadata"][scoreField] < maxUserScore) {
		        if (!(identifier in points)) {
                            var t = parseFloat(data[i]["time"]);
                            if (t >= minUserTime  &&  t <= maxUserTime) {
			        points[identifier] = new google.maps.Marker({"position": new google.maps.LatLng(data[i]["latitude"], data[i]["longitude"]), "map": map, "flat": true, "icon": circle});

			        google.maps.event.addListener(points[identifier], "click", function(ident) { return function() {
			            var obj = document.getElementById("table-" + ident);
			            obj.style.background = "#ffff00";
			            sidebar.scrollTop = obj.offsetTop;

			            var countdown = 10;
			            var state = true;
			            var callme = function() {
				        if (state) {
				            obj.style.background = null;
				            state = false;
				        }
				        else {
				            obj.style.background = "#ffff00";
				            state = true;
				        }

				        countdown--;
				        if (countdown >= 0) { setTimeout(callme, 200); }
			            };
			            setTimeout(callme, 200);

			        } }(identifier));
		            }
                        }
		    }
                }
	    }

	    stats_numPoints = Object.size(points);
	    updateStatus();
	}
    }
    xmlhttp.open("GET", url, true);
    xmlhttp.send();
}

function getPolygons() {
    var bounds = map.getBounds();
    if (!bounds) { return; }

    var depth = map.getZoom() - 2;
    if (depth != olddepth) {
        for (var key in polygons) {
            polygons[key].setMap(null);
            delete polygons[key];
        }
        polygons = {};
        dontReloadPolygons = {};
        setPolygonMetadata(null);
    }
    olddepth = depth;

    var longmin = bounds.getSouthWest().lng();
    var longmax = bounds.getNorthEast().lng();
    var latmin = bounds.getSouthWest().lat();
    var latmax = bounds.getNorthEast().lat();

    var tmp = tileIndex(10, longmin, latmin);
    longmin = tmp[1];
    latmin = tmp[2];
    tmp = tileIndex(10, longmax, latmax);
    longmax = tmp[1];
    latmax = tmp[2];

    var key = "" + depth + "-" + longmin + "-" + latmin + "-" + longmax + "-" + latmax;
    for (var oldkey in dontReloadPolygons) {
	if (key == oldkey) {
	    return;
	}
    }
    dontReloadPolygons[key] = true;

    var url = "../TileServer/getTile?command=polygons&longmin=" + longmin + "&longmax=" + longmax + "&latmin=" + latmin + "&latmax=" + latmax + "&polygonsTableName=MatsuLevel2Polygons";
    
    var xmlhttp = new XMLHttpRequest();
    xmlhttp.onreadystatechange = function() {
        if (xmlhttp.readyState == 4  &&  xmlhttp.status == 200) {
            if (xmlhttp.responseText != "") {
                var data = JSON.parse(xmlhttp.responseText)["data"];
                for (var i in data) {
                    var identifier = parseInt(data[i]["identifier"]);
                    if (!(identifier in polygons)) {
                        var t = parseFloat(data[i]["time"]);
                        if (t >= minUserTime  &&  t <= maxUserTime) {
                            var rawcoordinates = data[i]["polygon"];
                            var coordinates = [];
                            for (var j in rawcoordinates) {
                                coordinates.push(new google.maps.LatLng(rawcoordinates[j][1], rawcoordinates[j][0]));
                            }

                            polygons[identifier] = new google.maps.Polygon({"paths": coordinates, "strokeColor": "#ff0000", "strokeWeight": 2.0, "fillColor": "#ff0000", "fillOpacity": 0.0, "clickable": true});
                            polygons[identifier].setMap(map);

                            google.maps.event.addListener(polygons[identifier], "click", function(polys, ident, metadata) {
                                return function() {
                                    for (var otherid in polys) {
                                        polys[otherid].setOptions({"fillOpacity": 0.0});
                                    }
                                    polys[ident].setOptions({"fillOpacity": 0.25});
                                    setPolygonMetadata(metadata);
                                };
                            }(polygons, identifier, data[i]["metadata"]));
                        }
                    }
                }
            }

            stats_numPolygons = Object.size(polygons);
            updateStatus();
        }
    }
    xmlhttp.open("GET", url, true);
    xmlhttp.send();
}

function selectNoPolygons() {
    for (var key in polygons) {
        polygons[key].setOptions({"fillOpacity": 0.0});
    }
    setPolygonMetadata(null);
}

function prepareMetadata(metadata) {
    if (typeof metadata["L1T"] == "string") {
        metadata["L1T"] = JSON.parse(metadata["L1T"]);
    }
    return metadata;
}

function dumpMetadata(indentation, metadata) {
    if (typeof metadata == "string") {
        return metadata;
    }

    if (typeof metadata == "number") {
        return metadata.toString();
    }

    if (metadata instanceof Array) {
        return metadata.toString();
    }

    var keys = [];
    for (var key in metadata) {
        if (metadata.hasOwnProperty(key)) {
            keys.push(key);
        }
    }
    keys.sort();
    var output = "\n";
    for (var key in keys) {
        var value = metadata[keys[key]];
        output += indentation + "<b>" + keys[key] + "</b>: " + dumpMetadata(indentation + "    ", value) + "\n";
    }
    return output;
}

function setPolygonMetadata(metadata) {
    var infobox = document.getElementById("polygon-data");
    var selectnone = document.getElementById("unselect_button");
    if (metadata == null) {
        infobox.innerHTML = "<i>(none selected)</i>";
        selectnone.innerHTML = "";
    }
    else {
        metadata = prepareMetadata(metadata);

        var bandName = null;
        if ("L1T" in metadata  &&  "PRODUCT_METADATA" in metadata["L1T"]) {
            for (var key in metadata["L1T"]["PRODUCT_METADATA"]) {
                if (key.substr(0, 4) == "BAND"  &&  key.substr(-10) == "_FILE_NAME") {
                    bandName = metadata["L1T"]["PRODUCT_METADATA"][key].substr(0, 22);
                    break;
                }
            }
        }

        if (bandName != null) {
            var year = bandName.substr(10, 4);
            var jday = bandName.substr(14, 3);
            var glusterfs = null;
            if (bandName.substr(0, 4) == "EO1H") {
                glusterfs = "/glusterfs/matsu/eo1/hyperion_l1g/" + year + "/" + jday + "/" + bandName + "_HYP_L1G";
            }
            else {
                glusterfs = "/glusterfs/matsu/eo1/ali_l1g/" + year + "/" + jday + "/" + bandName + "_ALI_L1G";
            }

            var fileList = "udr rsync -av --stats --progress guest@opensciencedatacloud.org:" + glusterfs;
            var udr = "udr rsync -av --stats --progress guest@opensciencedatacloud.org:" + glusterfs + " .";
            var rsync = "rsync -avzu guest@opensciencedatacloud.org:" + glusterfs + " .";

            infobox.innerHTML = "<h4 style=\"margin-bottom: 10px;\">Download selected L1G image:</h4><div style=\"font-size: 11px\"><b>List of files from UDR<b><br><textarea style=\"width: 100%; resize: vertical; min-height: 31px;\" rows=\"1\" cols=\"20\" readonly=\"readonly\">" + fileList +  "</textarea><b>Download with UDR<b><br><textarea style=\"width: 100%; resize: vertical; min-height: 31px;\" rows=\"1\" cols=\"20\" readonly=\"readonly\">" + udr +  "</textarea><b>Download with plain rsync<b><br><textarea style=\"width: 100%; resize: vertical; min-height: 31px;\" rows=\"1\" cols=\"20\" readonly=\"readonly\">" + rsync +  "</textarea></div><h4>Selected L1 image metadata:</h4><pre>" + dumpMetadata("", metadata) + "</pre>";
        }
        else {
            infobox.innerHTML = "<h4>Selected L1 image metadata:</h4><pre>" + dumpMetadata("", metadata) + "</pre>";
        }

        selectnone.innerHTML = "<button onclick='selectNoPolygons()'>Unselect</button>";
    }
}

function updateStatus() {
    stats.innerHTML = "<span class='spacer'>Zoom depth: " + stats_depth + "</span><span class='spacer'>Tiles visible: " + stats_numVisible + "</span><span class='spacer'>Tiles in your browser's memory: " + stats_numInMemory + " (counting empty tiles)</span><span class='spacer'>Points: " + stats_numPoints + "</span><span class='spacer'>Polygons: " + stats_numPolygons + "</span>";
}

function switchTables(index) {
    if (index == 0) {
        minScore = 0.0;
        maxScore = 10.0;
        minUserScore = 5.0;
        maxUserScore = 10.0;
        scoreField = "analyticscore";
        pointsTableName = "None";
    } else if (index == 1) {
        minScore = 0.0;
        maxScore = 10.0;
        minUserScore = 0.0;
        maxUserScore = 10.0;
        scoreField = "analyticscore";
        pointsTableName = "MatsuLevel2LngLat";
    } else if (index == 2) {
        minScore = 0.0;
        maxScore = 30.0;
        minUserScore = 0.0;
        maxUserScore = 30.0;
        scoreField = "analyticsscore";
        pointsTableName = "MatsuLevel2Clusters";
    }

    getTable();

    for (var key in points) {
	points[key].setMap(null);
	delete points[key];
    }
    points = {};
    dontReloadPoints = {};

    if (showPoints) { getLngLatPoints(); }
    else { updateStatus(); }
}

// ]]>
    </script>
  </head>
  <body onload="initialize();" style="width: 100%; margin: 0px;">

  <div id="map_canvas" style="position: fixed; top: 5px; right: 5px; width: 100px; height: 100px; float: right; border: 1px solid black;"></div>
  <div id="sidebar" style="position: fixed; top: 5px; left: 5px; width: 300px; height: 100px; vertical-align: top; resize: horizontal; float: left; background: white; border: 1px solid black; padding: 5px; overflow-x: hidden; overflow-y: scroll;">

<h3 style="margin-top: 0px;">Timespan</h3>
<div style="position: relative; height: 20px; top: -10px;">
<div style="position: absolute; top: 0px; left: 10px; font-size: 11pt;">2012</div>
<div style="position: absolute; top: 0px; left: 50px; font-size: 11pt;">Apr</div>
<div style="position: absolute; top: 0px; left: 90px; font-size: 11pt;">Jul</div>
<div style="position: absolute; top: 0px; left: 130px; font-size: 11pt;">Oct</div>
<div style="position: absolute; top: 0px; left: 170px; font-size: 11pt;">2013</div>
<div style="position: absolute; top: 20px; left: 25px; width: 163px;"><div id="slider-time"></div></div>
</div>

<h3 style="margin-top: 20px;">Visible Layers</h3>
<form onsubmit="return false;">
<p class="layer_checkbox" onclick="toggleState('RGB', 'layer-RGB');"><label for="layer-RGB" onclick="toggleState('RGB', 'layer-RGB');"><input id="layer-RGB" class="layer-checkbox" type="checkbox" checked="true"> Canonical RGB</label>
<p class="layer_checkbox" onclick="toggleState('CO2', 'layer-CO2');"><label for="layer-CO2" onclick="toggleState('CO2', 'layer-CO2');"><input id="layer-CO2" class="layer-checkbox" type="checkbox" checked="true"> CO<sub>2</sub></label>
<p class="layer_checkbox" onclick="toggleState('flood', 'layer-flood');"><label for="layer-flood" onclick="toggleState('flood', 'layer-flood');"><input id="layer-flood" class="layer-checkbox" type="checkbox" checked="true"> <span style="color: red;">cloud</span>, <span style="color: green;">land</span>, <span style="color: blue;">water</span></label>
<p class="layer_checkbox" onclick="toggleState('TOPO_raw', 'layer-TOPO_raw');"><label for="layer-TOPO_raw" onclick="toggleState('TOPO_raw', 'layer-TOPO_raw');"><input id="layer-TOPO_raw" class="layer-checkbox" type="checkbox" checked="true"> Elevation (insensitive to timespan)</label>
</form>

<h3 style="margin-bottom: 0px;">Get Source Images</h3>
<form onsubmit="return false;">
<p class="layer_checkbox"><button onclick="updateDownloads();">Get visible images</button>
<select id="downloadstype-pulldown" onchange="setDownloadsType(this.selectedIndex);" style="float: right;">
<option value="l0">Level-0</option>
<option value="l1g" selected="true">Level-1G</option>
<option value="l1t">Level-1T</option>
</select>
<div style="margin-left: 20px; margin-top: 10px;">
<div style="font-size: 11px"><b>List all files using UDR<b><br>
<textarea id="download-filelist" style="width: 100%; resize: vertical; min-height: 31px;" rows="1" cols="20" readonly="readonly">(click button to update)</textarea>
<b>Download with UDR<b><br>
<textarea id="download-udr" style="width: 100%; resize: vertical; min-height: 31px;" rows="1" cols="20" readonly="readonly">(click button to update)</textarea>
<b>Download with plain rsync<b><br>
<textarea id="download-rsync" style="width: 100%; resize: vertical; min-height: 31px;" rows="1" cols="20" readonly="readonly">(click button to update)</textarea>
</div>
</div>
</form>

<h3 style="margin-bottom: 0px;">Latitude-longitude Points</h3>
<form onsubmit="return false;">
<p class="layer_checkbox" style="margin-bottom: 10px;">Source
<select id="points-pulldown" onchange="switchTables(this.selectedIndex);">
<option value="None" selected="true">none</option>
<option value="MatsuLevel2LngLat">limit-of-resolution entities</option>
<option value="MatsuLevel2Clusters">CO2 clusters</option>
</select>

<div id="points-stuff" style="margins: 0px; padding: 0px; visibility: visible;">
<p class="layer_checkbox"><label for="show-points"><input id="show-points" type="checkbox" checked="true" onclick="togglePoints('show-points');"> Show points</label>

<p style="margin-left: 20px; margin-bottom: 0px;">Filter by "analyticsscore"
<div style="margin-left: 25px; width: 163px; margin-top: 0px;"><div id="slider-points"></div></div>

<p id="table-here" class="layer_checkbox" style="margin-top: 10px;"></p>
</div>

</form>

<h3 style="margin-bottom: 0px;">Geospatial Polygons</h3>
<form onsubmit="return false;">
<div id="unselect_button" style="float: right;"></div>
<p class="layer_checkbox"><label for="show-polygons"><input id="show-polygons" type="checkbox" checked="false" onclick="togglePolygons('show-polygons');"> Show polygons</label>
</form>

<div id="polygon-data" style="margin-left: 20px;"><i>(none selected)</i></div>

</div>

  <div id="stats" style="position: fixed; bottom: 5px; width: 100%; text-align: center;"><span style="color: white;">No message</span></div>

  </body>
</html>
