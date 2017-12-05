<template>
	<div class="catena-data">
		<aside>
			<transition-group name="list" tag="ul">
				<li key="">
					<a title="Show grants" href="javascript:void(0);" v-if="database != ''" style="float:right;" @click.capture="describeDatabase(t)"><i class="fa fa-users"></i></a>
					<select v-model="database">
						<option key="" value="">Select database...</option>
						<option v-for="db in databases" :value="db" :key="db">{{db}}</option>
					</select>
				</li>

				<li v-for="(t, idx) in tables" @click.self="selectTable(t)" :class="{'selected': table === t}" :key="t">
					<i class="fa fa-table"></i> {{t}}
					<a title="Show table schema information" href="javascript:void(0);" style="float:right;" @click.capture="describeTable(t)"><i class="fa fa-info"></i></a>
				</li>

				<li v-for="(q, idx) in queries" @click="select(q)" :class="{'selected': query == q}" :key="idx">
					<a title="Remove" href="javascript:void(0);" style="float:right;" @click.capture="remove(idx)"><i class="fa fa-times"></i></a>
					<code>{{q}}</code>
				</li>
			</transition-group>
		</aside>
		<article style="overflow-y: auto;" v-if="database != ''">
			<textarea class="catena-sql" @keyup="setQuery" :value="typedQuery" @keyup.enter="enterUp"></textarea>
			<button @click="perform"><i class="fa fa-play"></i> Query</button>
			<catena-query :sql="query" v-if="query != '' && query !== null " :agent="agent" :head="head" :database="database"></catena-query>
		</article>
	</div>
</template>

<script>
const Agent = require("./blockchain").Agent;

module.exports = {
	props: {
		agent: Agent,
		head: String
	},
	
	data: function() {
		return {
			tables: [],
			databases: [],
			database: "",
			queries: ["CREATE TABLE foo(x INT);", "SELECT * FROM foo WHERE x < ?x;", "INSERT INTO foo (x) VALUES (?what);", "IF (?val*1) < 10 THEN INSERT INTO foo (x) VALUES (?val) ELSE FAIL END;"], 
			query: "SHOW TABLES;", 
			table: null,
			typedQuery: "SHOW TABLES;"
		};
	},

	watch: {
		head: function(nv) {
			this.refresh();
		},

		database: function(nv) {
			this.refreshTables();
		}
	},

	created: function() {
		this.refresh();
	},

	methods: {
		select: function(q) {
			this.query = q;
			this.table = null;
			this.typedQuery = q;
		},

		selectTable: function(t) {
			this.table = t;
			this.typedQuery = "SELECT * FROM \""+t+"\" LIMIT 50;";
			this.query = this.typedQuery;
		},

		describeDatabase: function(t) {
			this.table = null;
			this.typedQuery = "SHOW GRANTS;";
			this.query = this.typedQuery;
		},

		describeTable: function(t) {
			this.table = null;
			this.typedQuery = "DESCRIBE \""+t+"\";";
			this.query = this.typedQuery;
		},

		dropTable: function(t) {
			this.table = null;
			this.typedQuery = "DROP TABLE \""+t+"\";";
			this.query = this.typedQuery;
		},

		setQuery: function(e) {
			this.typedQuery = e.target.value;
			this.query = null;
			this.table = null;
		},

		refresh: function() {
			var self = this;

			this.agent.databases(function(err, databases) {
				databases.sort();
				self.databases = databases;
			});

			this.refreshTables();
		},

		refreshTables: function() {
			var self = this;

			if(this.database != "") {
				this.agent.tables(this.database, function(err, tables) {
					tables.sort();
					self.tables = tables;
				});
			}
			else {
				self.tables = [];
			}
		},

		remove: function(idx) {
			if(this.queries[idx] == this.query) {
				this.query = null;
			}
			this.queries.splice(idx, 1);
		},

		enterUp: function(e) {
			this.typedQuery = e.target.value;
			if(e.ctrlKey) {
				this.perform();
			}
		},

		perform: function() {
			this.query = this.typedQuery;
			let idx = this.queries.indexOf(this.query);
			if(idx == -1) {
				this.queries.push(this.query);
			}
		}
	}
};
</script>