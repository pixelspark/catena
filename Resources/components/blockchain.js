const ed25519 = require('supercop.js');
const base58check = require('base58check');
const bs58check = require('bs58check');
const sha256 = require('fast-sha256');
const toBuffer = require('typedarray-to-buffer');
const base64 = require('base64-js');
const Vue = require('vue');
Vue.use(require('vue-resource'));
const http = Vue.http;

class Identity {
	constructor(publicKey, privateKey) {
		this.publicKey = publicKey;
		this.privateKey = privateKey;
	}

	static generate() {
		const seed = ed25519.createSeed();
		const kp = ed25519.createKeyPair(seed);
		return new Identity(kp.publicKey, kp.secretKey);
	}

	static loadBase58(pub, sec) {
		let pubData = base58check.decode(pub);
		let secData = base58check.decode(sec);
		if(pubData.prefix[0] != 88) throw new Error("Invalid public key version");
		if(secData.prefix[0] != 11) throw new Error("Invalid private key version");

		let id = new Identity(pubData.data, secData.data);
		
		// Test signing
		let msg = new Buffer('hello there');
		if(!id.verify(msg, id.sign(msg))) {
			throw new Error("Invalid public/private key combination (sign/verify failed)");
		}
		
		return id;
	}

	get jsonObject() {
		return {publicKey: this.publicBase58, secretKey: this.privateBase58};
	}

	static fromJSON(js) {
		return Identity.loadBase58(js.publicKey, js.secretKey);
	}	

	persist(b) {
		let ids = {};
		if("catenaIdentities" in window.localStorage) {
			try {
				ids = JSON.parse(window.localStorage["catenaIdentities"]);
			}
			catch(e) {}
		}

		if(b) {
			ids[this.publicHash] = this.jsonObject;
		}
		else {
			delete ids[this.publicHash];
		}
		window.localStorage["catenaIdentities"] = JSON.stringify(ids);
	}

	get isPersisted() {
		if("catenaIdentities" in window.localStorage) {
			let ids = JSON.parse(window.localStorage["catenaIdentities"]);
			return this.publicHash in ids;
		}
		return false;
	}

	static persisted() {
		if("catenaIdentities" in window.localStorage) {
			try {
				let ids = JSON.parse(window.localStorage["catenaIdentities"]);
				var keys = [];

				for(var k in ids) {
					keys.push(Identity.fromJSON(ids[k]));
				}

				return keys;
			}
			catch(e) { console.log(e); }
		}
		return [];
	}

	sign(msg) {
		if(!Buffer.isBuffer(msg)) throw new Error("msg is not a buffer");
		
		let signature = ed25519.sign(msg, this.publicKey, this.privateKey);
		return signature;
	}

	verify(msg, sig) {
		return ed25519.verify(sig, msg, this.publicKey);
	}

	get publicHashHex() {
		let hex = atob(this.publicHash);
		return hexEncode(hex);
	}

	get publicHash() {
		if(this.publicKey !== null) {
			const hasher = new sha256.Hash();
			hasher.update(this.publicKey);
			const result = hasher.digest();
			return btoa(String.fromCharCode.apply(null, new Uint8Array(result)));
		}
		return null;
	}

	static prependVersion(buffer, version) {
		let versioned = new Uint8Array(buffer.length + 1);
		versioned[0] = version;
		for(var a=0; a<buffer.length; a++) {
			versioned[a+1] = buffer[a];
		}
		return versioned;
	}

	get publicBase58() {
		let versioned = new Buffer(Identity.prependVersion(this.publicKey, 88));
		return bs58check.encode(versioned);
	}

	get privateBase58() {
		let versioned = new Buffer(Identity.prependVersion(this.privateKey, 11));
		return bs58check.encode(versioned);
	}
}

function generateUUID() {
    var dt = new Date().getTime();
    var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = (dt + Math.random()*16)%16 | 0;
        dt = Math.floor(dt/16);
        return (c=='x' ? r :(r&0x3|0x8)).toString(16);
    });
    return uuid;
}

function hexEncode(str) {
	var hex, i;

    var result = "";
    for (i=0; i<str.length; i++) {
        hex = str.charCodeAt(i).toString(16);
        result += ("00"+hex).slice(-2);
    }

    return result;
}

class Transaction {
	constructor(invoker, counter, sql) {
		this.counter = counter;
		this.invoker = invoker;
		this.sql = sql;
		this.signature = null;
	}

	sign() {
		var data = this.dataForSigning;
		this.signature = this.invoker.sign(data);
	}

	verify() {
		let data = this.dataForSigning;
		return this.invoker.verify(data, this.signature);
	}

	get dataForSigning() {
		let text = new TextEncoder().encode(this.sql);
		var buffer = new ArrayBuffer(this.invoker.publicKey.length + 8 + text.length);
		var uints = new Uint8Array(buffer);
		
		var idx = 0;
		for(var a=0; a<this.invoker.publicKey.length; a++) {
			uints[idx] = this.invoker.publicKey[a];
			idx++;
		}

		// FIXME: we only support 32 bit counter values now.
		new DataView(buffer).setUint32(idx, this.counter, true /* littleEndian */);
		new DataView(buffer).setUint32(idx + 4, 0, true /* littleEndian */);
		idx += 8;
		
		for(var a=0; a<text.length; a++) {
			uints[idx] = text[a];
			idx++;
		}
		
		var realBuffer = new Buffer(idx);
		for(var a=0; a<idx; a++) {
			realBuffer[a] = uints[a];
		}
		return realBuffer;
	}

	get jsonObject() {
		var d = {
			tx: {
				sql: this.sql,
				counter: this.counter,
				invoker: this.invoker.publicBase58
			}
		};

		if(this.signature !== null) {
			d.signature = base64.fromByteArray(this.signature);
		}

		return d;
	}
}

/** Gossip connection client. */
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

class Agent {
	constructor(url, ws) {
		this.url = url;
		this.connection = ws;
		this.identities = Identity.persisted();
	}

	counter(publicBase58, callback) {
		http.get(this.url + "/api/counter/" + escape(publicBase58)).then(function(r) {
			if(r.ok && r.status == 200) {
				callback(null, r.body.counter);
			}
			else {
				callback(null);
			}
		}, function(r) {
			callback(null);
		});
	}
}

module.exports = {
	Identity: Identity,
	Connection: Connection,
	Agent: Agent,
	generateUUID: generateUUID,
	Transaction: Transaction
};