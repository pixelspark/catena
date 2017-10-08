<template>
	<span>
		<code v-if="expanded">{{hash}}</code>
		<abbr :title="hash" v-if="!expanded" @click="expand(true)">{{shortHash}}</abbr>
	</span>
</template>

<script>
module.exports = {
	props: {
		hash: {
			type: String
		},

		expandable: {
			type: Boolean, 
			default: true 
		}
	},

	data: function() {
		return {expanded: false};
	},

	methods: {
		expand: function(e) {
			if(this.expandable) {
				this.expanded = e;
			}
		}
	},

	computed: {
		shortHash: function() {
			return this.hash.substr(0,5) + "…"+this.hash.substr(-5);
		}
	}
};
</script>