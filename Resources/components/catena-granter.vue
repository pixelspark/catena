<template>
	<div>
		<dl v-if="database === null">
			<dt>Database</dt>
			<dd>
				<select v-model="selectedDatabase">
					<option value="" key="">Select...</option>
					<option v-for="db in databases" :value="db" :key="db">{{db}}</option>
				</select>
			</dd>
		</dl>

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
				<p class="info" v-else-if="hasParameters">The grantee will be able to execute the query above with any value(s) for the parameter(s) <b>{{parametersFriendly}}</b>. </p>
				<button @click="updateTemplateHash"><i class="fa fa-check"></i> Use template</button>
			</dd>
		</dl>

		<dl v-if="kind != 'template'">
			<dt><input type="radio" value="all" v-model="tableType"> All tables in the database</dt>
			<dt><input type="radio" value="existing" v-model="tableType"> Existing table</dt>
			<dd v-if="tableType == 'existing'">
				<select v-model="table">
					<option :value="null">Select...</option>
					<option v-for="t in tables" :value="t" :key="t">{{t}}</option>
				</select>
			</dd>

			<dt><input type="radio" value="other" v-model="tableType"> Other table</dt>
			<dd v-if="tableType == 'other'"><input type="text" v-model="table" placeholder="table"></dd>
		</dl>

		<catena-transaction :sql="sql" v-if="tableType !== null && kind !== null" :agent="agent" :database="selectedDatabase"></catena-transaction>
	</div>
</template>

<script>
const Agent = require("./blockchain").Agent;
const Identity = require("./blockchain").Identity;

module.exports = {
	props: {
		agent: Agent,
		user: Identity,
		database: {type: String, default: null},
		kinds: {default: function() { return {
			"insert": "Insert",
			"delete": "Delete",
			"update": "Update",
			"drop": "Drop",
			"create": "Create",
			"template": "Template",
			"grant": "Grant"
		}; } }
	},

	data: function() {
		return { 
			kind: "insert",
			tables: [],
			selectedDatabase: this.database,
			databases: [],
			table: null,
			tableType: null,
			templateQuery: "",
			templateHash: null,
			templateError: null,
			templateParameters: null,
		};
	},

	watch: {
		selectedDatabase: function() {
			this.refresh();
		},
		database: function(nv) {
			this.selectedDatabase = nv;
			this.refresh();
		},
	},

	created: function() {
		this.refresh();
	},

	computed: {
		sql: function() {
			if(this.kind == 'template') {
				return "GRANT template X'"+this.templateHash+"' TO X'"+this.user.publicHashHex+"';";
			}
			else {
				if(this.table === null || this.tableType == 'all') {
					return "GRANT "+this.kind+" TO X'"+this.user.publicHashHex+"';";
				}
				else {
					return "GRANT "+this.kind+" ON \""+this.table+"\" TO X'"+this.user.publicHashHex+"';";
				}
			}
		},

		hasParameters: function() {
			if(this.templateParameters === null) return false;
			for(var k in this.templateParameters) {
				return true;
			}
			return false;
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
			this.agent.query(this.templateQuery, {}, self.database, function(code, res) {
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

			this.agent.databases(function(err, databases) {
				databases.sort();
				self.databases = databases;
			});

			if(self.database != "") {
				this.agent.tables(self.selectedDatabase, function(code, tbls) {
					self.tables = tbls;
				});
			}
			else {
				self.tables = [];
			}
		}
	}
};
</script>