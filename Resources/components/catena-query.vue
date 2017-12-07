<template>
	<div class="catena-query">
		<p class="error" v-if="error !== null"><i class="fa fa-warning"></i> {{$t('error')}} {{error}}</p>
		<catena-spinner v-else-if="isLoading" fill="rgb(0,55,100)">{{$t('loading')}}</catena-spinner>
		<div v-else>
			<dl v-if="hasParameters">
				<h2>{{$t('parametrizedQuery')}}</h2>
				<p class="info" v-if="isUnbound"><i class="fa fa-info"></i> {{$t('parameterDescription')}}</p>

				<template v-for="(v, k) in parameters">
					<dt :key="k">{{k}}</dt>
					<dd :key="k"><input style="width: 100%;" type="text" :value="v" @keyup="setParameter(k, $event.target.value)"/></dd>
				</template>
			</dl>

			<button v-if="hasParameters" @click="update"><i class="fa fa-check"></i> Apply</button>

			<template v-if="isMutating">
				<h2>{{$t('mutatingQuery')}}</h2>
				<p>{{$t('mutatingQueryDescription')}}</p>
				<catena-expander :title="$t('createTransaction')" icon="plus">
					<catena-transaction :sql="formattedSQL" :agent="agent" :database="database"></catena-transaction>
				</catena-expander>
			</template>

			<table v-if="!isMutating && result !== null">
				<thead>
					<tr>
						<th v-for="col in result.columns" :key="col">{{col}}</th>
					</tr>
				</thead>

				<tbody>
					<tr v-if="!result.rows || result.rows.length == 0" ><td>{{noDataText()}}</td></tr>

					<tr v-for="(row, idx) in result.rows" :key="idx">
						<td v-for="(cell, idx) in row" :key="idx">
							<span v-if="cell === null" class="null">NULL</span>
							<span v-else>
								{{cell}}
							</span>
						</td>
					</tr>
				</tbody>
			</table>
		</div>
	</div>
</template>

<script>
const Agent = require("./blockchain").Agent;

module.exports = {
	props: {
		sql: String,
		database: String,
		agent: Agent,
		head: { type: String, default: null },
		noData: { type: String, default: null }
	},

	data: function() {
		return { 
			result: null, 
			error: null, 
			isLoading: false,
			isMutating: false, 
			isUnbound: false,
			parameters: {},
			formattedSQL: null,
		};
	},

	watch: {
		sql: function(nv) {
			this.parameters = null;
			this.update();
		},

		database: function(nv) {
			this.update();
		},

		head: function() {
			this.update(true);
		}
	},

	created: function() {
		this.update();
	},

	computed: {
		hasParameters: function() {
			if(this.parameters === null) {
				return false
			}

			for(var k in this.parameters) {
				return true
			}

			return false
		}
	},

	methods: {
		noDataText: function() {
			if(this.noData !== null) {
				return this.noData;
			}
			return this.$t('noData');
		},

		setParameter: function(k, v) {
			this.parameters[k] = v;
		},

		update: function(silent) {
			var self = this;
			self.error = null;
			self.result = null;
			if(!silent) {
				self.isLoading = true;
			}

			this.agent.query(this.sql, this.parameters, this.database, function(code, res) {
				self.isLoading = false;

				if(code == 200) {
					self.isMutating = false;
					self.isUnbound = false;
					self.error = null;
					self.result = res;
				}
				else if(code == 406) {
					self.parameters = res.parameters;

					if("unbound" in res) {
						for(var a=0; a<res.unbound.length; a++) {
							self.parameters[res.unbound[a]] = null;
						}
						self.isUnbound = true;
					}
					else {
						// Mutating query
						self.isUnbound = false;
						self.isMutating = true;
						self.formattedSQL = res.sql;
					}
				}
				else if(res && ("message" in res)) {
					self.error = res.message;
				}
				else {
					self.err = res.statusText;
				}
			});
		}
	},

	i18n: { messages: {
		en: {
			mutatingQuery: "Mutating query",
			mutatingQueryDescription: "The query you have submitted is a mutating query. Do you want to submit a transaction to execute the query?",
			createTransaction: "Create transaction",
			parametrizedQuery: "Parameterized query",
			parameterDescription: "Set these parameters first!",
			loading: "Loading...",
			error: "Error:",
			noData: "(No data)",
		},

		nl: {
			mutatingQuery: "Wijzigingsopdracht",
			mutatingQueryDescription: "De opdracht die is ingevoerd wijzigt (mogelijk) gegevens. Om de wijzigingen te kunnen doervoeren dient de opdracht als transactie te worden ingezonden.",
			createTransaction: "Maak transactie",
			parametrizedQuery: "Opdractparameters",
			parameterDescription: "Om deze opdracht uit te kunnen voeren, dienen eerst de volgende parameters te worden gekozen.",
			loading: "Bezig met laden...",
			error: "Fout:",
			noData: "(Geen gegevens)",
		}
	} },
};
</script>