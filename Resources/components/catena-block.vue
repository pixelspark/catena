<template>
	<li class="catena-block" @click="select">
		<div style="float:right;">
			<slot></slot>
		</div>

		<h3>
			<i class="fa fa-cube" aria-hidden="true"></i>
			Block #{{block.index}}:
			<template v-if="isGenesis">
				(Genesis)
			</template>
			<catena-hash :hash="block.hash" :expandable="false"></catena-hash><br/>
		</h3>

		<i class="fa fa-clock-o" aria-hidden="true"></i> <catena-timestamp :timestamp="block.timestamp"></catena-timestamp>
	</li>
</template>

<script>
module.exports = {
	  props: {
		  block: {type: Object}
	  },

	  computed: {
		  isGenesis: function() {
			  return this.block.previous == "0000000000000000000000000000000000000000000000000000000000000000"
		  },

		  payload: function() {
			  try {
				  return JSON.parse(atob(this.block.payload));
			  }
			  catch(e) { return []; }
		  }
	  },

	  methods: {
		  select: function() {
			  this.$emit('select');
		  }
	  }
};
</script>