<template>
	<div :class="{'catena-tab':true, 'selected': selected}">
		<slot></slot>
	</div>
</template>

<script>
module.exports = {
	props: {
		name: String
	},

	data: function() {
		return {selected: false};
	},

	created: function() {
		this.$parent.tabs.push(this);
		this.selected = this.$parent.tabs.length == 1;
	},

	computed: {
		index: function() {
			return this.$parent.tabs.indexOf(this);
		}
	},

	watch: {
		'$parent.currentTab' (index) {
			this.selected = this.index === index;
		}
	}
};
</script>