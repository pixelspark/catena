<template>
	<abbr :title="hash">{{shortHash}}</abbr>
</template>

<script>
module.exports = {
	  props: {
		  hash: {type: String}
	  },

	  computed: {
		  shortHash: function() {
			  return this.hash.substr(0,5) + "..."+this.hash.substr(-5);
		  }
	  }
};
</script>