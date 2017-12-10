<template>
	<div class="catena-transaction">
		<dl>
			<dt>{{$t('statement', {database: database})}}</dt>
			<dd><code>{{sql}}</code></dd>

			<dt>{{$t('invoker')}}</dt>
			<dd>
				<select @change="updateInvoker" :value="invoker" :disabled="submitting || submitted">
					<option :value="null">{{$t('select')}}</option>
					<option v-for="(identity, idx) in agent.identities" v-if="identity.privateKey !== null" :value="idx" :key="identity.publicHash">{{identity.publicHash}}</option>
				</select><br/>
				<template v-if="counter !== null">
					<template>{{$t('count', {counter: counter})}}</template>
				</template>
			</dd>

		<template v-if="counter !== null && !submitted">
			<dt></dt>
			<dd>
				<button @click="submit" :disabled="submitting || submitted"><i class="fa fa-check"></i> {{$t('signAndSubmit')}}</button>
				<button @click="sign" :disabled="submitting || submitted || transaction !== null"><i class="fa fa-check"></i> {{$t('sign')}}</button>
			</dd>
		</template>

		<template v-if="submitted">
			<dt></dt>
			<dd>
				<strong>{{$t('submitted')}}</strong>
				<button @click="reset"><i class="fa fa-restart"></i> Restart</button>
			</dd>
		</template>

		<template v-if="transaction !== null">
			<dt></dt>
			<dd>
				<textarea readonly class="catena-code" v-model="transactionJSONString"></textarea>
			</dd>
		</template>
	</div>
</template>

<script>
const Agent = require('./blockchain').Agent;
const Transaction = require('./blockchain').Transaction;

module.exports = {
	props: {
		sql: String,
		database: String,
		agent: Agent
	},

	data: function() {
		return {invoker: null, counter: null, submitting: false, submitted: false, transaction: null };
	},

	i18n: { messages: {
		en: {
			submitted: "Submitted!",
			sign: "Sign",
			signAndSubmit: "Sign and submit",
			invoker: "Invoker",
			statement: "Statement to be executed on database '{database}':",
			select: "Select...",
			count: "This will be transaction #{counter} for this invoker."
		},

		nl: {
			submitted: "Verzonden!",
			sign: "Onderteken",
			signAndSubmit: "Onderteken en verzend",
			invoker: "Opdrachtgever",
			statement: "Opdracht die dient te worden uitgevoerd op database '{database}':",
			select: "Selecteer...",
			count: "Dit wordt transactie #{counter} voor deze uitvoerder."
		}
	} },

	watch: {
		sql: function(nv) {
			this.reset();
		}
	},

	computed: {
		transactionJSONString: function() {
			return this.transaction == null ? "" : JSON.stringify(this.transaction.jsonObject);
		},
	},

	methods: {
		sign: function() {
			let id = this.agent.identities[this.invoker];
			this.transaction = new Transaction(id, this.database, this.counter, this.sql);
			this.transaction.sign();
			if(!this.transaction.verify()) {
				throw new Error("my own transaction should verify");
			}
		},

		submit: function() {
			this.submitting = true;
			this.sign();

			var gossip = {
				t: "tx",
				tx: this.transaction.jsonObject
			};

			this.agent.connection.request(gossip, function(res) {
			});

			this.submitted = true;
		},

		reset: function() {
			this.submitted = false;
			this.submitting = false;
			this.transaction = null;
			this.refresh();
		},

		refresh: function() {
			var self = this;

			if(this.invoker !== null) {
				let id = this.agent.identities[this.invoker];
				this.agent.counter(id.publicBase58, function(err, ctr) {
					if(ctr === null || typeof(ctr) == 'undefined') {
						self.counter = 0;
					}
					else {
						self.counter = ctr + 1;
					}
				});
			}
		},

		updateInvoker: function(e) {
			var self = this;
			this.invoker = e.target.value;
			this.counter = null;
			this.refresh();
		}
	}
};
</script>