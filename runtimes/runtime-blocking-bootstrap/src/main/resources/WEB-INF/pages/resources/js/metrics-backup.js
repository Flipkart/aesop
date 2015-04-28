
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

			init: function(elm,views) {
			    for (var i in views) {
			        var view = views[i];
                    $(elm).append('<div class="section"><h2>'+view.title+'</h2><div class="metrics-container" id="'+view.id+'"></div></div>');
                    this.views.push(view);
			    }
				this.es = new EventSource("/metrics-stream");
				this.es.onmessage = function(evt) {
					var data = $.parseJSON(evt.data);
					for (var i in Metrics.views) {
                        Metrics.refresh(views[i].id,views[i].metrics,data);
					}
				};
			},

            views: [],
			gauges: {},
			counters: {},
			histograms: {},
			meters: {},
			timers: {},

			refresh: function(elm_id,views,data) {

                for (var i in views) {

                    var view = views[i];
                    var id = this.generateID(view.id);
                    var type = view.type + "s";
                    var elm = document.getElementById(elm_id);

                    if (!elm || !data[type][view.id]) return;

                    if (this[type][id]) {
                        this[type][id].update(data[type][view.id]);
                    } else {
                        if (view.type == "gauge") {
                            this[type][id] = new Gauge(id, view.name, elm, data[type][view.id]);
                        } else if (view.type == "counter") {
                            this[type][id] = new Counter(id, view.name, elm, data[type][view.id]);
                        } else if (view.type == "meter") {
                            this[type][id] = new Meter(id, view.name, elm, data[type][view.id]);
                        } else if (view.type == "histogram") {
                            this[type][id] = new Histogram(id, view.name, elm, data[type][view.id]);
                        } else if (view.type == "timer") {
                            this[type][id] = new Timer(id, view.name, elm, data[type][view.id]);
                        }
                    }

                }

			},

			generateID: function(name) {
				return name.replace(/\./g,"_").replace(/[^\w]/gi, '');
			},

			getUnitTemplate: function(id, name) {
				var html = "";
				html += "<div class='unit'>";
				html += "<div class='name' title='"+name+"' alt='"+name+"'>"+name+"</div>";
				html += "<div class='plot' id='plot-"+id+"'></div>";
				html += "<div class='metrics' id='metrics-"+id+"'></div>";
				html += "</div>"
				return html;
			}

		};

		function Histogram(id, name, elm, data) {
			this.data = {
				mean: []
			};
			this.series = {
				mean: {
					color: "#005387",
					data: [],
					shadowSize: 0,
					label: "Mean"
				}
			};

			this.update = function(data) {
				
				this.data.mean.splice(0,1);
				this.data.mean.push(data.mean);

				this.series.mean.data = [];
				for (var i = 0; i < this.data.mean.length; ++i) {
					this.series.mean.data.push([i, this.data.mean[i]]);
				}
				
				this.plot.setData([this.series.mean]);
				this.plot.setupGrid();
				this.plot.draw();

				$("#metrics-"+id).html(this.getMetricsAsHtml(data));

			}

			this.getMetricsAsHtml = function(data) {
				var html = "<table border='0'>";
				html += "<tr>";
				html += "<td class='key'>Count</td><td class='val'>" + parseInt(data.count) + "</td>";
				html += "<td class='key'>95th</td><td class='val'>" + parseInt(data.p95) + "ms</td>";
				html += "</tr>";
				html += "<tr>";
				html += "<td class='key'>Median</td><td class='val'>" + parseInt(data.p50) + "ms</td>";
				html += "<td class='key'>99th</td><td class='val'>" + parseInt(data.p99) + "ms</td>";
				html += "</tr>";
				html += "<tr>";
				html += "<td class='key'>Mean</td><td class='val'>" + parseInt(data.mean) + "ms</td>";
				html += "<td class='key'>99.9th</td><td class='val'>" + parseInt(data.p999) + "ms</td>";
				html += "</tr>";
				html += "</table>";
				return html;
			}

			for (var i = 0; i < Config.maxPoints; i++) {
				this.data.mean.push(null);
				this.series.mean.data.push([i,null]);
			}
		
			$(elm).append(Metrics.getUnitTemplate(id,name));
			this.plot = $.plot("#plot-"+id, [this.series.mean], Config.options);

			this.update(data);

		}

		function Counter(id, name, elm, data) {
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

			this.update = function(data) {
				
				this.data.rate.splice(0,1);
				this.data.rate.push(data.count - this.data.count);

				this.series.rate.data = [];
				for (var i = 0; i < this.data.rate.length; ++i) {
					this.series.rate.data.push([i, this.data.rate[i]]);
				}
				
				this.plot.setData([this.series.rate]);
				this.plot.setupGrid();
				this.plot.draw();

				$("#metrics-"+id).html(this.getMetricsAsHtml(data));

				this.data.count = data.count;

			}

			this.getMetricsAsHtml = function(data) {
				var delta = data.count-this.data.count;
				var html = "<table border='0'>";
				html += "<tr><td class='key'>Count</td><td class='val'>" + parseInt(data.count) + " ("+(delta>0?"+"+delta:delta)+")</td></tr>";
				html += "</table>";
				return html;
			}

			for (var i = 0; i < Config.maxPoints; i++) {
				this.data.rate.push(null);
				this.series.rate.data.push([i,null]);
			}
		
			$(elm).append(Metrics.getUnitTemplate(id, name));
			this.plot = $.plot("#plot-"+id, [this.series.rate], Config.options);

			this.update(data);

		}

		function Meter(id, name, elm, data) {
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

			this.update = function(data) {
				
				this.data.rate.splice(0,1);
				this.data.rate.push(data.count - this.data.count);

				this.series.rate.data = [];
				for (var i = 0; i < this.data.rate.length; ++i) {
					this.series.rate.data.push([i, this.data.rate[i]]);
				}
				
				this.plot.setData([this.series.rate]);
				this.plot.setupGrid();
				this.plot.draw();

				$("#metrics-"+id).html(this.getMetricsAsHtml(data));

				this.data.count = data.count;

			}

			this.getMetricsAsHtml = function(data) {
				var html = "<table border='0'>";
				html += "<tr>";
				html += "<td class='key'>Count</td><td class='val'>" + parseInt(data.count) + "</td>";
				html += "</tr>";
				html += "<tr>";
				html += "<td class='key'>Rate (5min)</td><td class='val'>" + parseInt(data.m5_rate) + "/sec</td>";
				html += "</tr>";
				html += "<tr>";
				html += "<td class='key'>Rate (1min)</td><td class='val'>" + parseInt(data.m1_rate) + "/sec</td>";
				html += "</tr>";
				html += "<tr>";
				html += "<td class='key'>Rate (15min)</td><td class='val'>" + parseInt(data.m15_rate) + "/sec</td>";
				html += "</tr>";
				html += "</table>";
				return html;
			}

			for (var i = 0; i < Config.maxPoints; i++) {
				this.data.rate.push(null);
				this.series.rate.data.push([i,null]);
			}
		
			$(elm).append(Metrics.getUnitTemplate(id, name));
			this.plot = $.plot("#plot-"+id, [this.series.rate], Config.options);

			this.update(data);

		}


		function Gauge(id, name, elm, data) {
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

			this.update = function(data) {
				
				this.data.value.splice(0,1);
				this.data.value.push(data.value);

				this.series.value.data = [];
				for (var i = 0; i < this.data.value.length; ++i) {
					this.series.value.data.push([i, this.data.value[i]]);
				}
				
				this.plot.setData([this.series.value]);
				this.plot.setupGrid();
				this.plot.draw();

				$("#metrics-"+id).html(this.getMetricsAsHtml(data));

			}

			this.getMetricsAsHtml = function(data) {
				var html = "<table border='0'>";
				html += "<tr><td class='key'>Value</td><td class='val'>" + parseInt(data.value) + "</td></tr>";
				html += "</table>";
				return html;
			}

			for (var i = 0; i < Config.maxPoints; i++) {
				this.data.value.push(null);
				this.series.value.data.push([i,null]);
			}
		
			$(elm).append(Metrics.getUnitTemplate(id, name));
			this.plot = $.plot("#plot-"+id, [this.series.value], Config.options);

			this.update(data);

		}

		function Timer(id, name, elm, data) {
			this.data = {
				rate: [],
				mean: [],
				count: 0
			};
			this.options = {
				xaxis: {
					show: false
				},
				yaxis: {
					show: false
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
			this.series = {
				rate: {
					color: "#005387",
					data: [],
					shadowSize: 0,
					label: "Rate"
				},
				mean: {
					color: "#005387",
					data: [],
					shadowSize: 0,
					label: "Mean"
				}
			};

			this.update = function(data) {
				
				this.data.rate.splice(0,1);
				this.data.rate.push(data.count - this.data.count);
				this.series.rate.data = [];
				for (var i = 0; i < this.data.rate.length; ++i) {
					this.series.rate.data.push([i, this.data.rate[i]]);
				}
				this.data.count = data.count;

				this.data.mean.splice(0,1);
				this.data.mean.push(data.mean);
				this.series.mean.data = [];
				for (var i = 0; i < this.data.mean.length; ++i) {
					this.series.mean.data.push([i, this.data.mean[i]]);
				}
				
				this.plot.setData([this.series.mean]);
				this.plot.setupGrid();
				this.plot.draw();

				$("#metrics-"+id).html(this.getMetricsAsHtml(data));

			}

			this.getMetricsAsHtml = function(data) {
				var html = "<table border='0'>";
				html += "<tr>";
				html += "<td class='key'>Count</td><td class='val'>" + data.count + "</td>";
				html += "<td class='key'>Rate</td><td class='val'>" + parseInt(data.m1_rate) + "/sec</td>";
				html += "</tr>";
				html += "<tr>";
				html += "<td class='key'>Median</td><td class='val'>" + parseInt(data.p50) + "ms</td>";
				html += "<td class='key'>95th</td><td class='val'>" + parseInt(data.p95) + "ms</td>";
				html += "</tr>";
				html += "<tr>";
				html += "<td class='key'>Mean</td><td class='val'>" + parseInt(data.mean) + "ms</td>";
				html += "<td class='key'>99th</td><td class='val'>" + parseInt(data.p99) + "ms</td>";
				html += "</tr>";
				html += "</table>";
				return html;
			}

			for (var i = 0; i < Config.maxPoints; i++) {
				this.data.rate.push(null);
				this.series.rate.data.push([i,null]);
				this.data.mean.push(null);
				this.series.mean.data.push([i,null]);
			}
		
			$(elm).append(Metrics.getUnitTemplate(id, name));
			this.plot = $.plot("#plot-"+id, [this.series.mean], Config.options);

			this.update(data);

		}

