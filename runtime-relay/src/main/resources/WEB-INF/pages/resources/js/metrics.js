
		Config = {
			maxPoints: 60,
			options: {
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
					backgroundColor: "#6FC3E9",
					borderWidth: 0
				},
				legend: {
					margin: 0,
					labelBoxBorderColor: "none"
				}
			}
		};

		Metrics = {

			init: function() {
				this.es = new EventSource("/metrics-stream");
				this.es.addEventListener('message', function(evt) {
					var data = $.parseJSON(evt.data);
                    Metrics.refresh(data);
				}, false);
			},

			plots: {},

			views: {
			    "io": {
			        container: "io-stats-container",
                    views: {
                        "numDataEvents": {
                            title: "Number of Data Events",
                            plot: "counter"
                        },
                        "sizeDataEvents": {
                            title: "Size of Data Events",
                            plot: "gauge"
                        },
                        "maxSeenWinScn": {
                            title: "SCN Update Rate",
                            plot: "counter"
                        },
                        "timeSinceLastEvent": {
                            title: "Time since last event",
                            plot: "gauge"
                        }
                    }
			    },
			    "relay": {
			        container: "relay-stats-container",
			        views: {
                        "freeSpace": {
                            title: "Free Space in Buffer",
                            plot: "gauge"
                        },
                        "timeSpan": {
                            title: "Time Span within Buffer",
                            plot: "gauge"
                        },
                        "timeLag": {
                            title: "Time Lag",
                            plot: "gauge"
                        }
                    }
			    },
			    "http": {
			        container: "http-stats-container",
                    views: {
                        "maxStreamWinScn": {
                            title: "SCN Update Rate",
                            plot: "counter"
                        },
                        "numStreamCalls": {
                            title: "Number of /stream Calls",
                            plot: "counter"
                        },
                        "latencyStreamCalls": {
                            title: "Latency of /stream calls",
                            plot: "gauge"
                        },
                        "httpErrorRate": {
                            title: "HTTP Error Rate",
                            plot: "gauge"
                        }
                    }
			    }

			},

			refresh: function(data) {
                for (var type in this.views) {
                    var container = document.getElementById(this.views[type].container);
                    for (var key in this.views[type].views) {
                        var title = this.views[type].views[key].title;
                        var plot = this.views[type].views[key].plot;
                        if (type == "io") {
                            this.refreshIOStats(container, key, title, plot, data["inbound"][key], data["outbound"][key]);
                        } else if (type == "relay") {
                            this.refreshRelayStats(container, key, title, plot, data["inbound"][key]);
                        } else if (type == "http" ) {
                            this.refreshHttpStats(container, key, title, plot, data["http"][key]);
                        }
                    }
                }

			},

			refreshIOStats: function(container, key, title, plot, dataIn, dataOut) {
			    if (!container) return;
			    var pair = document.getElementById("pair-"+key);
			    if (!pair) {
                    $(container).append("<div class='unit-pair' id='pair-"+key+"'><div class='title'>"+title+"</div><div class='left' id='pair-"+key+"-left'></div><div class='right' id='pair-"+key+"-right'></div></div>")
			    }
			    if (dataIn != null) this.plotData(document.getElementById("pair-"+key+"-left"), "pair-"+key+"-inbound", "Inbound", plot, dataIn);
			    if (dataOut != null) this.plotData(document.getElementById("pair-"+key+"-right"), "pair-"+key+"-outbound", "Outbound", plot, dataOut);
			},
			refreshRelayStats: function(container, key, title, plot, data) {
			    if (!container) return;
                if (data != null) this.plotData(container, "http-"+key, title, plot, data);
			},
			refreshHttpStats: function(container, key, title, plot, data) {
			    if (!container) return;
                if (data != null) this.plotData(container, "http-"+key, title, plot, data);
			},

			plotData: function(container, id, title, plot, data) {

                // create plots if required
                if (!this.plots[id]) {
                    if (plot == "gauge") {
                        this.plots[id] = new Gauge(id, title, container);
                    } else if (plot == "counter") {
                        this.plots[id] = new Counter(id, title, container);
                    } else {
                        console.log("Invalid plot type: " + plot);
                    }
                } else {
                    // update plots
                    if (this.plots[id]) {
                        this.plots[id].update(data);
                    }
                }

			},

			getUnitTemplate: function(id, name) {
				var html = "";
				html += "<div class='unit'>";
				html += "<div class='name' title='"+name+"' alt='"+name+"'>"+name+": <span id='metrics-"+id+"'></span></div>";
				html += "<div class='plot' id='plot-"+id+"'></div>";
				html += "</div>"
				return html;
			}

		};

		function Counter(id, name, elm) {
			this.data = {
				rate: [],
				count: 0
			};
			this.series = {
				rate: {
					color: "#005387",
					data: [],
					shadowSize: 0,
					label: "Rate"
				}
			};

			this.update = function(count) {

				this.data.rate.splice(0,1);
				this.data.rate.push(count - this.data.count);

				this.series.rate.data = [];
				for (var i = 0; i < this.data.rate.length; ++i) {
					this.series.rate.data.push([i, this.data.rate[i]]);
				}

				this.plot.setData([this.series.rate]);
				this.plot.setupGrid();
				this.plot.draw();

				var delta = count - this.data.count;
				$("#metrics-"+id).html(parseInt(count) + " ("+(delta>0?"+"+delta:delta)+")");

				this.data.count = count;

			}

			for (var i = 0; i < Config.maxPoints; i++) {
				this.data.rate.push(null);
				this.series.rate.data.push([i,null]);
			}

			$(elm).append(Metrics.getUnitTemplate(id, name));
			this.plot = $.plot("#plot-"+id, [this.series.rate], Config.options);

		}

		function Gauge(id, name, elm) {
			this.data = {
				value: []
			};
			this.series = {
				value: {
					color: "#005387",
					data: [],
					shadowSize: 0,
					label: "Value"
				}
			};

			this.update = function(value) {

				this.data.value.splice(0,1);
				this.data.value.push(value);

				this.series.value.data = [];
				for (var i = 0; i < this.data.value.length; ++i) {
					this.series.value.data.push([i, this.data.value[i]]);
				}

				this.plot.setData([this.series.value]);
				this.plot.setupGrid();
				this.plot.draw();

				$("#metrics-"+id).html(parseInt(value));

			}

			for (var i = 0; i < Config.maxPoints; i++) {
				this.data.value.push(null);
				this.series.value.data.push([i,null]);
			}

			$(elm).append(Metrics.getUnitTemplate(id, name));
			this.plot = $.plot("#plot-"+id, [this.series.value], Config.options);

		}

