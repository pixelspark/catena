<template>
	<span>{{friendly}}</span>
</template>

<script>
module.exports = {
	template: '#catena-timestamp',
	props: {
		timestamp: {type: Number}
	},

	computed: {
		friendly: function() {
			let d = new Date(this.timestamp * 1000);
			return d.toLocaleString();
		}
	}
};
</script>