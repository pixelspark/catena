<template>
	<div class="catena-data">
		<aside>
			<ul>
				<li v-for="(t, idx) in tables" @click="selectTable(t)" :class="{'selected': table === t}" :key="t">
					<i class="fa fa-table"></i> {{t}}
				</li>

				<li v-for="(q, idx) in queries" @click="select(q)" :class="{'selected': query == q}" :key="idx">
					<a href="javascript:void(0);" style="float:right;" @click="remove(idx)"><i class="fa fa-times"></i></a>
					<code>{{q}}</code>
				</li>
			</ul>
		</aside>
		<article style="overflow-y: auto;">
			<textarea @keyup="setQuery" :value="typedQuery" @keyup.enter="enterUp"></textarea>
			<button @click="perform"><i class="fa fa-play"></i> Query</button>
			<p></p>
			<catena-query :sql="query" v-if="query != '' " :agent="agent"></catena-query>
		</article>
	</div>
</template>

<script>
const Agent = require("./blockchain").Agent;

module.exports = {
	props: {
		agent: Agent
	},
	
	data: function() {
		return {
			tables: [],
			queries: ["CREATE TABLE foo(x INT);"], 
			query: "SHOW TABLES;", 
			table: null,
			typedQuery: "SHOW TABLES;"
		};
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
			this.typedQuery = "SELECT * FROM "+t+";";
			this.query = this.typedQuery;
		},

		setQuery: function(e) {
			this.typedQuery = e.target.value;
			this.table = null;
		},

		refresh: function() {
			var self = this;

			this.agent.tables(function(err, tables) {
				tables.sort();
				self.tables = tables;
			});
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