<template>
	<div class="catena-identities">
		<aside>
			<transition-group name="list" tag="ul">
				<li v-for="(q, idx) in agent.identities" @click="select(q)" :class="{'selected': identity == q}" :key="idx">
					<a href="javascript:void(0);" style="float:right;" @click="remove(idx)"><i class="fa fa-times"></i></a>
					<i v-if="q.privateKey !== null" class="fa fa-key"></i>
					<i v-else class="fa fa-user"></i>
					<catena-hash :hash="q.publicHash" format="base64" :expandable="false"></catena-hash>
				</li>
			</transition-group>

			<button style="float: right; margin: 5px;" @click="generate"><i class="fa fa-plus"></i> {{$t('generateIdentity')}}</button>
			<button style="float: right; margin: 5px;" @click="load"><i class="fa fa-download"></i> {{$t('importIdentity')}}</button>
		</aside>
		<article style="overflow-y: auto;">
			<div v-if="identity === null">
				<h1>{{$t('loadAKey')}}</h1>
				
				<dl>
					<dt>{{$t('publicKey')}}</dt>
					<dd><input type="text" style="width: 100%;" :value="newPublic" @keyup="updatePublic"></dd>

					<dt>{{$t('privateKey')}}</dt>
					<dd><input type="text" style="width: 100%;" :value="newPrivate" @keyup="updatePrivate"></dd>

					<dt></dt>
					<dd><button @click="loadKey"><i class="fa fa-check"></i> {{$t('load')}}</button></dd>
				</dl>
			</div>
			<catena-identity v-if="identity !== null" :identity="identity" :agent="agent" :head="head"></catena-identity>
		</article>
	</div>
</template>

<script>
const Identity = require("./blockchain").Identity;
const Agent = require("./blockchain").Agent;

module.exports = {
	props: {
		agent: Agent,
		head: {type: String, default: null}
	},
	
	data: function() {
		return {
			identity: null,
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
				this.identity = Identity.loadBase58(this.newPublic, this.newPrivate == "" ? null : this.newPrivate);
				this.agent.identities.push(this.identity);
			}
			catch(e) {
				alert(e);
			}
		},

		remove: function(idx) {
			let id = this.agent.identities[idx];
			id.persist(false);

			if(this.identity !== null && (this.agent.identities[idx].publicHash == this.identity.publicHash)) {
				this.identity = null;
			}
			this.agent.identities.splice(idx, 1);

		},

		generate: function() {
			let id = Identity.generate();
			this.agent.identities.push(id);
			this.identity = id;
			
		}
	},

	i18n: { messages: {
		en: {
			loadAkey: "Load an identity...",
			load: "Load",
			generateIdentity: "Generate new identity",
			importIdentity: "Import identity",
			privateKey: "Secret key",
			publicKey: "Public key",
		},
		nl: {
			loadAKey: "Laad een identiteit...",
			load: "Laad",
			generateIdentity: "Genereer een nieuwe identiteit",
			importIdentity: "Importeer identiteit",
			privateKey: "Geheime sleutel",
			publicKey: "Publieke sleutel",
		}
	} }
};
</script>