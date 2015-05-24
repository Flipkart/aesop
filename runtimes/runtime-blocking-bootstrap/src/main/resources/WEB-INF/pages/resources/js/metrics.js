
Config = {
    maxPoints: 60,
    options: {
        default: {
            xaxis: {
                show: false
            },
            yaxis: {
                show: true,
                font: {
                    size: 8,
                    lineHeight: 9,
                    color: "#AAA"
                }
            },
            grid: {
                show: true,
                backgroundColor: "#FFF",
                borderWidth: 1
            },
            legend: {
                margin: 0,
                position: "nw",
                labelBoxBorderColor: "none"
            }
        },
        combo: {
            xaxis: {
                show: true
            },
            yaxis: {
                show: true,
                font: {
                    size: 9,
                    lineHeight: 9,
                    color: "#777"
                }
            },
            grid: {
                show: true,
                borderWidth: 1
            },
            legend: {
                margin: 0,
                position: "nw",
                labelBoxBorderColor: "none"
            }

        }
    }
};

Models = [
    {
        type: "combo",
        title: "Producer / Client Stats",
        data: [
            {
                title: "Last Seen SCN",
                type: "combo",
                id: "combo1",
                data: {
                    "producer": "gauge",
                }
            }
        ]
    },
    {
        type: "pairs",
        title: "Inbound vs Outbound Stats",
        data: [
            {
                title: "Number of Data Events",
                first: {
                    title: "Inbound",
                    type: "counter",
                    data: "inbound.numDataEvents"
                },
                second: {
                    title: "Outbound",
                    type: "counter",
                    data: "outbound.numDataEvents"
                }
            },
            {
                title: "Size of Data Events",
                first: {
                    title: "Inbound",
                    type: "gauge",
                    data: "inbound.sizeDataEvents"
                },
                second: {
                    title: "Outbound",
                    type: "gauge",
                    data: "outbound.sizeDataEvents"
                }
            },
            {
                title: "SCN Update Rate",
                first: {
                    title: "Inbound",
                    type: "counter",
                    data: "inbound.maxSeenWinScn"
                },
                second: {
                    title: "Outbound",
                    type: "counter",
                    data: "outbound.maxSeenWinScn"
                }
            },
            {
                title: "Time Since Last Event",
                first: {
                    title: "Inbound",
                    type: "gauge",
                    data: "inbound.timeSinceLastEvent"
                },
                second: {
                    title: "Outbound",
                    type: "gauge",
                    data: "outbound.timeSinceLastEvent"
                }
            }
        ]
    },
    {
        type: "simple",
        title: "Relay Stats",
        data: [
            {
                title: "Free Space in Buffer",
                type: "gauge",
                data: "inbound.freeSpace"
            },
            {
                title: "Time Span within Buffer",
                type: "gauge",
                data: "inbound.timeSpan"
            },
            {
                title: "Time Lag",
                type: "gauge",
                data: "inbound.timeLag"
            }
        ]
    },
    {
        type: "simple",
        title: "HTTP (Outbound) Stats",
        data: [
            {
                title: "SCN Update Rate",
                type: "counter",
                data: "http.maxStreamWinScn"
            },
            {
                title: "Number of /stream Calls",
                type: "counter",
                data: "http.numStreamCalls"
            },
            {
                title: "Latency of /stream calls",
                type: "gauge",
                data: "http.latencyStreamCalls"
            },
            {
                title: "HTTP Error Rate",
                type: "gauge",
                data: "http.httpErrorRate"
            }
        ]
    }
]

Views = {
    base: function(model) {
        var html = '';
        html += '<div class="section">';
        html += '<h2>'+model.title+'</h2>';
        switch (model.type) {
            case "combo":
                html += Views.comboList(model.data);
                break;
            case "pairs":
                html += Views.pairList(model.data);
                break;
            case "simple":
                html += Views.graphList(model.data);
                break;
        }
        html += '</div>';
        return html;
    },
    comboList: function(model) {
        var html = "";
        for (var i in model) {
            html += Views.combo(model[i]);
        }
        return html;
    },
    combo: function(model) {
        return  "<div class='combo'>" +
            "<div class='name' title='"+model.title+"' alt='"+model.title+"'>"+model.title+"</div>" +
            "<div class='plot' id='plot-"+model.id+"' style='width:600px;height:300px;'></div>" +
            "</div>";

    },
    pairList: function(model) {
        var html = "";
        for (var i in model) {
            html += Views.pair(model[i]);
        }
        return html;
    },
    pair: function(model) {
        return  "<div class='unit-pair'>" +
            "<div class='title'>"+model.title+"</div>" +
            "<div class='left'>"+Views.graph(model.first)+"</div>" +
            "<div class='right'>"+Views.graph(model.second)+"</div>" +
            "</div>";
    },
    graphList: function(model) {
        var html = "";
        for (var i in model) {
            html += Views.graph(model[i]);
        }
        return html;
    },
    graph: function(model) {
        return  "<div class='unit'>" +
            "<div class='name' title='"+model.title+"' alt='"+model.title+"'>" +
            model.title+": <span id='metrics-"+model.data+"'></span>" +
            "</div>" +
            "<div class='plot' id='plot-"+model.data+"' style='width:250px;height:125px;'></div>" +
            "</div>";
    }
}

Dashboard = {

    plots: {},

    init: function(ele) {

        var eventSourceUri = "/metrics-stream";
        // start events stream
        this.es = new EventSource(eventSourceUri);
        this.es.addEventListener('message', function(evt) {
            var data = $.parseJSON(evt.data);
            Dashboard.refresh(data);
        }, false);

        // initialize templates
        for (var i in Models) {
            $(ele).append(Views.base(Models[i]));
        }

        // initialize graphs
        for (var i in Models) {
            switch (Models[i].type) {
                case "pairs":
                    for (var j in Models[i].data) {
                        Dashboard.initPlot(Models[i].data[j].first);
                        Dashboard.initPlot(Models[i].data[j].second);
                    }
                    break;
                case "combo":
                case "simple":
                default:
                    for (var j in Models[i].data) {
                        Dashboard.initPlot(Models[i].data[j]);
                    }
                    break;
            }
        }

    },

    initPlot: function(plot) {
        switch (plot.type) {
            case "combo":
                Dashboard.plots[plot.id] = new Combo(plot.data);
                break;
            case "gauge":
                Dashboard.plots[plot.data] = new Gauge();
                break;
            case "counter":
                Dashboard.plots[plot.data] = new Counter();
                break;
            default:
                console.log("Invalid plot type: " + plot.type);
                break;
        }
    },

    refresh: function(data) {
        for (var i in data) {
            for (var j in data[i]) {
                var idx = i + "." + j;
                if (Dashboard.plots[idx] != null) {
                    Dashboard.plots[idx].update(data[i][j]).draw(idx);
                }
            }
        }
        // special handling for combo as it requires different type of data
        for (var i in Models) {
            if (Models[i].type == "combo") {
                for (var j in Models[i].data) {
                    var combo = Models[i].data[j];
                    // data to be sent to update
                    var map = {};
                    for (var key in combo.data) {
                        map[key] = data[key];
                    }
                    // update
                    Dashboard.plots[combo.id].update(map).draw(combo.id);
                }
            }
        }
    }



};

function Combo(d) {

    this.defs = d;
    this.plots = [];
    this.series = [];

    this.update = function(data) {
        this.series = [];
        for (var key in this.defs) {
            for (var name in data[key]) {
                var idx = key + "." + name;
                if (!this.plots[idx]) {
                    switch (this.defs[key]) {
                        case "gauge": this.plots[idx] = new Gauge(); break;
                        case "counter": this.plots[idx] = new Counter(); break;
                    }
                }
                this.plots[idx].update(data[key][name]);
                this.series.push({
                    data: this.plots[idx].getSeries(),
                    shadowSize: 0,
                    label: key + ":" + name
                });
            }
        }
        return this;
    }

    this.draw = function(id) {
        if (!this.plot) {
            var ele = document.getElementById("plot-"+id);
            this.plot = $.plot(ele, this.series, Config.options.combo);
        } else {
            this.plot.setData(this.series);
            this.plot.setupGrid();
            this.plot.draw();
        }
        return this;
    }
}

function Counter() {

    this.history = [];
    this.count = 0;
    this.delta = 0;

    // initial values
    for (var i = 0; i < Config.maxPoints; i++) {
        this.history.push(null);
    }

    this.getSeries = function() {
        var series = [];
        for (var i = 0; i < this.history.length; ++i) {
            series.push([i, this.history[i]]);
        }
        return series;
    };

    this.update = function(c) {

        // HACK - to counter stat resets
        if (c == 0) c = this.count;

        // update data array
        this.history.splice(0,1);
        this.history.push(c - this.count);

        // update last count & delta
        this.delta = c - this.count;
        this.count = c;


        return this;

    };

    this.draw = function(id) {

        var series = {
            data: this.getSeries(),
            shadowSize: 0,
            label: "Rate"
        };

        // draw plot
        if (!this.plot) {
            var ele = document.getElementById("plot-"+id);
            this.plot = $.plot(ele, [series], Config.options.default);
        } else {
            this.plot.setData([series]);
            this.plot.setupGrid();
            this.plot.draw();
        }

        // update text value
        $(document.getElementById("metrics-"+id)).html(parseInt(this.count) + " ("+(this.delta>0?"+"+this.delta:this.delta)+")");

        return this;

    };

}

function Gauge() {

    this.history = [];
    this.value = 0;

    // initial values
    for (var i = 0; i < Config.maxPoints; i++) {
        this.history.push(null);
    }

    this.getSeries = function() {
        var series = [];
        for (var i = 0; i < this.history.length; ++i) {
            series.push([i, this.history[i]]);
        }
        return series;
    };

    this.update = function(value) {

        // HACK - to counter stats reset
        if (value == -1) value = this.value;

        // update data array
        this.history.splice(0,1);
        this.history.push(value);

        // update value
        this.value = value;

        return this;

    };

    this.draw = function(id) {

        // series required by flot
        var series = {
            data: this.getSeries(),
            shadowSize: 0,
            label: "Value"
        };

        // draw plot
        if (!this.plot) {
            var ele = document.getElementById("plot-"+id);
            this.plot = $.plot(ele, [series], Config.options.default);
        } else {
            this.plot.setData([series]);
            this.plot.setupGrid();
            this.plot.draw();
        }

        // update text
        $(document.getElementById("metrics-"+id)).html(parseInt(this.value));

        return this;

    };

}
