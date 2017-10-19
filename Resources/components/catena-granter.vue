<template>
	<div>
		<dl>
			<dt>Kind</dt>
			<dd>
				<select v-model="kind">
					<option v-for="(v, k) in kinds" :value="k" :key="k">{{v}}</option>
				</select>
			</dd>
		</dl>

		<dl v-if="kind == 'template'">
			<dt>Template query</dt>
			<dd>
				<textarea class="catena-sql" v-model="templateQuery" @keyup="resetTemplateHash" style="width: 100%; height: 100px;"></textarea>
				<p class="error" v-if="templateError !== null">{{templateError}}</p>
				<p class="info" v-else-if="templateParameters !== null">The grantee will be able to execute the query above with any value(s) for the parameter(s) <b>{{parametersFriendly}}</b>. </p>
				<button @click="updateTemplateHash"><i class="fa fa-check"></i> Use template</button>
			</dd>
		</dl>

		<dl v-if="kind != 'template'">
			<dt>Table</dt>
			<dd>
				<select v-model="table">
					<option :value="null">Select...</option>
					<option v-for="t in tables" :value="t" :key="t">{{t}}</option>
				</select>
			</dd>
		</dl>

		<catena-transaction :sql="sql" v-if="(table !== null || templateHash !== null) && kind !== null" :agent="agent"></catena-transaction>
	</div>
</template>

<script>
const Agent = require("./blockchain").Agent;
const Identity = require("./blockchain").Identity;

module.exports = {
	props: {
		agent: Agent,
		user: Identity,
		kinds: {default: function() { return {
			"insert": "Insert",
			"delete": "Delete",
			"update": "Update",
			"drop": "Drop",
			"create": "Create",
			"template": "Template"
		}; } }
	},

	data: function() {
		return { 
			kind: "insert",
			tables: [],
			table: null,
			templateQuery: "",
			templateHash: null,
			templateError: null,
			templateParameters: null,
		};
	},

	created: function() {
		this.refresh();
	},

	computed: {
		sql: function() {
			if(this.kind == 'template') {
				return "INSERT INTO grants (\"kind\", \"user\", \"table\") VALUES ('"+this.kind+"', X'"+this.user.publicHashHex+"', X'"+this.templateHash+"');";
			}
			else {
				return "INSERT INTO grants (\"kind\", \"user\", \"table\") VALUES ('"+this.kind+"', X'"+this.user.publicHashHex+"', '"+this.table+"');";
			}
		},

		parametersFriendly: function() {
			var items = [];
			for(var k in this.templateParameters) {
				items.push(k);
			}
			return items.join(", ");
		}
	},

	methods: {
		resetTemplateHash: function() {
			this.templateHash = null;
			this.templateParameters = null;
		},

		updateTemplateHash: function() {
			var self = this;
			self.templateParameters = null;
			self.templateError = null;
			self.templateHash = null;
			this.agent.query(this.templateQuery, {}, function(code, res) {
				if(code == 200) {
					self.templateError = "This is not a mutating query!";
				}
				else if(code == 406) {
					self.templateHash = res.templateHash;	
					self.templateQuery = res.template;
					self.templateParameters = res.parameters;
				}
				else {
					self.templateError = ("message" in res) ? res.message : "unknown error";
				}
			});
		},

		refresh: function() {
			var self = this;

			this.agent.tables(function(code, tbls) {
				self.tables = tbls;
			});
		}
	}
};
</script>