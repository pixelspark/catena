<template>
	<div :class="{'catena-tabs':true, 'first-tab-selected': currentTab == 0}">
		<ul>
			<li 
				v-for="(tab, index) in tabs"
				@click="select(index)"
				:class="{'selected': currentTab == index}" :key="index">
				{{tab.name}}
			</li>
		</ul>

		<div>
			<slot></slot>
		</div>
	</div>
</template>

<script>
module.exports = {
	data: function() {
		return {tabs: [], currentTab: this.value};
	},

	props: {
		value: {type: Number, default: 0}
	},

	watch: {
		value: function(nv) {
			this.currentTab = nv;
		}
	},
	
	methods: {
		select: function(idx) {
			this.currentTab = idx;
			this.$emit('input',idx);
		}
	}
};
</script>