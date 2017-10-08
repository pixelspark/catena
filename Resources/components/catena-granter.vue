<template>
	<div class="catena-transaction">
		<dl>
			<dt>Kind</dt>
			<dd>
				<select v-model="kind">
					<option v-for="(v, k) in kinds" :value="k" :key="k">{{v}}</option>
				</select>
			</dd>
		</dl>

		<dl>
			<dt>Table</dt>
			<dd>
				<select v-model="table">
					<option :value="null">Select...</option>
					<option v-for="t in tables" :value="t" :key="t">{{t}}</option>
				</select>
			</dd>
		</dl>

		<catena-transaction :sql="sql" v-if="table !== null && kind !== null" :agent="agent"></catena-transaction>
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
			"drop": "Drop",
			"create": "Create"
		}; } }
	},

	data: function() {
		return { 
			kind: "insert",
			tables: [],
			table: null
		};
	},

	created: function() {
		this.refresh();
	},

	computed: {
		sql: function() {
			return "INSERT INTO grants (\"kind\", \"user\", \"table\") VALUES ('"+this.kind+"', X'"+this.user.publicHashHex+"', '"+this.table+"');";
		}
	},

	methods: {
		refresh: function() {
			var self = this;

			this.agent.tables(function(err, tbls) {
				self.tables = tbls;
			});
		}
	}
};
</script>