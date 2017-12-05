<template>
	<div>
		<textarea class="catena-code" v-model="transaction"></textarea>
		<p class="error" v-if="error !== null">{{error}}</p>
		<button @click="submit"><i class="fa fa-check"></i> {{$t('submit')}}</button>
	</div>
</template>

<script>
const Agent = require("./blockchain").Agent;

module.exports = {
	props: {
		agent: {type: Agent}
	},

	data: function() {
		return {
			transaction: "",
			error: null
		};
	},

	i18n: { messages: {
		en: {
			submit: "Submit"
		},

		nl: {
			submit: "Verzend"
		}
	} },

	methods: {
		submit: function() {
			try {
				this.error = null;
				var gossip = {
					t: "tx",
					tx: JSON.parse(this.transaction)
				};

				this.agent.connection.request(gossip, function(res) {
				});

				this.transaction = "";
			}
			catch(e) {
				this.error = e.toLocaleString();
			}
		}
	}
};
</script>