<template>
	<div class="catena-identities">
		<aside>
			<ul>
				<li v-for="(q, idx) in identities" @click="select(q)" :class="{'selected': identity == q}" :key="idx">
					<a href="javascript:void(0);" style="float:right;" @click="remove(idx)"><i class="fa fa-times"></i></a>
					<i class="fa fa-user"></i>
					<catena-hash :hash="q.publicHash"></catena-hash>
				</li>
			</ul>

			<button style="float: right; margin: 5px;" @click="generate"><i class="fa fa-plus"></i> Generate new identity</button>
			<button style="float: right; margin: 5px;" @click="load" v-if="identity !== null"><i class="fa fa-plus"></i> Import</button>
		</aside>
		<article style="overflow-y: auto;">
			<div v-if="identity === null">
				<h1>Load a key...</h1>
				
				<dl>
					<dt>Public key</dt>
					<dd><input type="text" style="width: 100%;" :value="newPublic" @keyup="updatePublic"></dd>

					<dt>Private key</dt>
					<dd><input type="text" style="width: 100%;" :value="newPrivate" @keyup="updatePrivate"></dd>

					<dt></dt>
					<dd><button @click="loadKey"><i class="fa fa-check"></i> Load</button></dd>
				</dl>
			</div>
			<catena-identity v-if="identity !== null" :identity="identity" :agent="agent"></catena-identity>
		</article>
	</div>
</template>

<script>
const Identity = require("./blockchain").Identity;
const Agent = require("./blockchain").Agent;

module.exports = {
	props: {
		agent: Agent
	},
	
	data: function() {
		return {
			identity: null,
			identities: Identity.persisted(),
			newPrivate: "",
			newPublic: ""
		};
	},

	methods: {
		updatePrivate: function(e) {
			this.newPrivate = e.target.value;
		},

		updatePublic: function(e) {
			this.newPublic = e.target.value;
		},

		select: function(q) {
			this.identity = q;
		},

		load: function() {
			this.identity = null;
		},

		loadKey: function() {
			try {
				this.identity = Identity.loadBase58(this.newPublic, this.newPrivate);
				this.identities.push(this.identity);
			}
			catch(e) {
				alert(e);
			}
		},

		remove: function(idx) {
			this.identity.persist(false);
			
			if(this.identity !== null && (this.identities[idx].publicHash == this.identity.publicHash)) {
				this.identity = null;
			}
			this.identities.splice(idx, 1);

		},

		generate: function() {
			let id = Identity.generate();
			this.identities.push(id);
			this.identity = id;
			
		}
	}
};
</script>