<template>
	<span>
		<code v-if="expanded">{{hash}}</code>
		<abbr :title="hash" v-if="!expanded" @click="expand(true)">{{shortHash}}</abbr>
	</span>
</template>

<script>
module.exports = {
	  props: {
		  hash: {type: String}
	  },

	  data: function() {
		  return {expanded: false};
	  },

	  methods: {
		  expand: function(e) {
			  this.expanded = e;
		  }
	  },

	  computed: {
		  shortHash: function() {
			  return this.hash.substr(0,5) + "…"+this.hash.substr(-5);
		  }
	  }
};
</script>