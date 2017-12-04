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
		if(this.$parent.currentTab == "") this.$parent.currentTab = this.name.toLowerCase();
		this.selected = this.$parent.currentTab.toLowerCase() == this.name.toLowerCase();
	},

	computed: {
		index: function() {
			return this.$parent.tabs.indexOf(this);
		}
	},

	watch: {
		'$parent.currentTab' (name) {
			this.selected = this.name.toLowerCase() == name.toLowerCase();
		}
	}
};
</script>