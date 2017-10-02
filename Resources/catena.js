function generateUUID() {
    var dt = new Date().getTime();
    var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = (dt + Math.random()*16)%16 | 0;
        dt = Math.floor(dt/16);
        return (c=='x' ? r :(r&0x3|0x8)).toString(16);
    });
    return uuid;
}

Vue.component('catena-payload', {
	template: '#catena-payload',
	props: {
		payload: {type: String}
	},

	computed: {
		seed: function() {
		try {
			let e = JSON.parse(atob(this.payload));
			return null;
			}
			catch(e) {
				return atob(this.payload);
			}
		},

		data: function() {
			try {
			return JSON.parse(atob(this.payload));
			}
			catch(e) {
				return [];
			}
		}
	}
});

Vue.component('catena-chain', {
	template: '#catena-chain',
	props: {
		hash: String,
		connection: Object,
		selectedHash: String
	},

	data: function() {
		return {
			first: null,
			blocks: []
		};
	},

	watch: {
		hash: function(nv) {
			this.update();
		}
	},

	created: function() {
		this.update();
	},

	methods: {
		update: function() {
			var self = this;
			
			var blocks = [];
			var count = 0;
			var limit = 100;
			var selectedSeen = false;
			function fetch(h) {
				self.connection.fetch(h, function(b) {
					if(self.first === null) {
						self.first = b.index;
					}

					count++;

					blocks.push(b);
					if(b.index > self.first && count < limit) {
						fetch(b.previous);
					}
					else {
						self.blocks = blocks;
					}
				});
			}

			fetch(this.hash);
		},

		select: function(hash) {
			var self = this;

			self.connection.fetch(hash, function(b) {
				self.$emit('select', b);
			});
		}
	}
});

Vue.component('catena-block', {
	  template: '#catena-block',
	  props: {
		  block: {type: Object}
	  },

	  computed: {
		  isGenesis: function() {
			  return this.block.previous == "0000000000000000000000000000000000000000000000000000000000000000"
		  },

		  payload: function() {
			  try {
				  return JSON.parse(atob(this.block.payload));
			  }
			  catch(e) { return []; }
		  }
	  },

	  methods: {
		  select: function() {
			  this.$emit('select');
		  }
	  }
});

Vue.component('catena-block-details', {
	template: '#catena-block-details',
	props: {
		block: {type: Object}
	},

	methods: {
		select: function(hash) {
			this.$emit('select', hash);
		}
	}
});

Vue.component('catena-hash', {
	  template: '#catena-hash',
	  props: {
		  hash: {type: String}
	  },

	  computed: {
		  shortHash: function() {
			  return this.hash.substr(0,5) + "..."+this.hash.substr(-5);
		  }
	  }
});

Vue.component('catena-timestamp', {
	template: '#catena-timestamp',
	props: {
		timestamp: {type: Number}
	},

	computed: {
		friendly: function() {
			let d = new Date(this.timestamp * 1000);
			return d.toLocaleString();
		}
	}
});

Vue.component('catena-tabs', {
	template: '#catena-tabs',
	data: function() {
		return {tabs: [], currentTab: 0};
	},
	
	methods: {
		select: function(idx) {
			this.currentTab = idx;
		}
	}
});

Vue.component('catena-tab', {
	template: '#catena-tab',
	props: {
		name: String
	},

	data: function() {
		return {selected: false};
	},

	created: function() {
		this.$parent.tabs.push(this);
		this.selected = this.$parent.tabs.length == 1;
	},

	computed: {
		index: function() {
			return this.$parent.tabs.indexOf(this);
		}
	},

	watch: {
		'$parent.currentTab' (index) {
			this.selected = this.index === index;
		}
	}
});

class Connection {
	constructor(url, onConnect) {
		var self = this;
		this.counter = 0;
		this.callbacks = {};
		this.onReceiveBlock = null;
		this.blocks = {};

		this.socket = new WebSocket(url, "catena-v1");
		this.socket.onopen = onConnect;
		this.socket.onmessage = function(x) { self.onReceive(x); };
	}

	disconnect() {
		this.socket.close();
	}

	fetch(blockHash, callback) {
		var self = this;

		if(blockHash in this.blocks) {
			return callback(this.blocks[blockHash]);
		}
		else {
			this.request({t:"fetch", hash: blockHash}, function(response) {
				self.blocks[blockHash] = response.block;
				return callback(response.block);
			});
		}
	}
	
	onReceive(r) {
		try {
			var data = JSON.parse(r.data);
			if(!Array.isArray(data)) { console.log("Receive invalid: ", data); return; }
			
			if(data[0] in this.callbacks) {
				// Solicited
				this.callbacks[data[0]](data[1]);
				delete this.callbacks[data[0]];
			}
			else {
				// Unsolicited
				var self = this;
				
				var replyFunction = function(response) {
					self.connection.send(JSON.stringify([data[0], response]));
				};
				
				this.onReceiveUnsolicited(data[1], replyFunction);
			}
			
		}
		catch(e) {
			console.log("PARSE ERROR: ", r.data, e);
		}
	}
	
	onReceiveUnsolicited(d, reply) {
		switch(d.t) {
			case "query":
				// Indicate we are a passive peer
				reply({t: "passive"});
				
		
			case "block":
				if(this.onReceiveBlock) {
					this.blocks[d.block.hash] = d.block;
					this.onReceiveBlock(d.block);	
				}
				break;
				
			case "tx":
				break;

			case "forget":
				console.log("Peer requests to be forgotton. Its UUID does not match!", d);
				
			default:
				console.log("Unknown unsolicited gossip: ", d);
			
		}
	}
	
	request(req, callback) {
		this.counter += 2;
		this.callbacks[this.counter] = callback;
		this.socket.send(JSON.stringify([this.counter, req]));
	}
};

Vue.component('catena-data', {
	template: '#catena-data',
	props: {
		url: String
	},
	
	data: function() {
		return {
			queries: ["SHOW TABLES;", "SELECT * FROM grants;"], 
			query: "SHOW TABLES;", 
			typedQuery: "SHOW TABLES;"
		};
	},

	methods: {
		select: function(q) {
			this.query = q;
			this.typedQuery = q;
		},

		setQuery: function(e) {
			this.typedQuery = e.target.value;
		},

		remove: function(idx) {
			if(this.queries[idx] == this.query) {
				this.query = "";
			}
			this.queries.splice(idx, 1);
		},

		perform: function() {
			this.query = this.typedQuery;
			let idx = this.queries.indexOf(this.query);
			if(idx == -1) {
				this.queries.push(this.query);
			}
		}
	}
});

Vue.component('catena-query', {
	template: '#catena-query',
	props: {
		sql: String,
		url: String
	},

	data: function() {
		return {result: null, error: null};
	},

	watch: {
		sql: function(nv) {
			this.update();
		}
	},

	created: function() {
		this.update();
	},

	methods: {
		update: function() {
			var self = this;
			self.error = null;
			self.result = null;
			var data = {sql: this.sql};
			this.$http.post(this.url + "/api/query", data).then(function(r) {
				if(r.ok && r.status == 200) {
					console.log('got', r.body);
					self.result = r.body;
				}
				else {
					self.error = r.bodyText;
				}
			}, function(r) {
				if("message" in r.body) {
					self.error = r.body.message;
				}
				else {
					self.err = r.statusText;
				}
			});
		}
	}
});

var app = new Vue({
	el: '#app',
	
	data: function() {
		var initial = {
			url: window.location.host,
			uuid: generateUUID(),
			index: null,
			connection: null,
			transactions: [],
			selectedBlock: null
		};

		try {
			if("catena" in window.localStorage) {
				var saved = JSON.parse(window.localStorage.catena);
				for(var k in saved) {
					if(saved.hasOwnProperty(k)) {
						initial[k] = saved[k];
					}
				}
			}
		}
		catch(e) {}
		return initial;
	},

	created: function() {
		this.connect();
	},

	computed: {
		dataURL: function() {
			return "http://" + this.url;
		}
	},

	methods: {
		select: function(block) {
			this.selectedBlock = block;
		},

		goToBlock: function(evt) {
			this.selectHash(evt.target.value);
		},

		connect: function() {
			try {
				var self = this;

				var url = "ws://" + this.url + "?uuid=" + this.uuid;
				this.connection = new Connection(url, function() {
					self.update();
				});

				this.connection.onReceiveBlock = function(x) {
					self.onReceiveBlock(x);
				};
			}
			catch(e) {
				alert("Error connecting: "+e);
			}	
		},

		disconnect: function() {
			this.connection.disconnect();
			this.connection = null;
			this.index = null;
			this.selectedBlock = null;
		},

		update: function() {
			var self = this;

			self.connection.request({t:"query"}, function(response) { 
				if(response.t == "index") {
					self.index = response.index;
				}
			});
		},

		selectHash: function(hash) {
			var self = this;
			self.selectedBlock = null;

			self.connection.fetch(hash, function(b) {
				self.selectedBlock = b;
			});
		},

		onReceiveBlock: function(b) {
			if(this.index != null && b.index > this.index.height) {
				this.update();
			}
		},
		
		set: function(attr, value) {
			var persistedAttributes = ["url"];
			this[attr] = value;
			var data = {};
			var self = this;
			persistedAttributes.forEach(function(k) {
				data[k] = self[k];
			});
			window.localStorage.catena = JSON.stringify(data);
		}
	}
});
