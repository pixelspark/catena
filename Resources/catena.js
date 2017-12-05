const Vue = require('vue');
const VueInt = require('vue-i18n');
Vue.use(require('vue-resource'));
Vue.use(VueInt);

const App = require('./components/catena.vue');
Vue.component('catena-block', require('./components/catena-block.vue'));
Vue.component('catena-blocks', require('./components/catena-blocks.vue'));
Vue.component('catena-block-details', require('./components/catena-block-details.vue'));
Vue.component('catena-chain', require('./components/catena-chain.vue'));
Vue.component('catena-data', require('./components/catena-data.vue'));
Vue.component('catena-expander', require('./components/catena-expander.vue'));
Vue.component('catena-granter', require('./components/catena-granter.vue'));
Vue.component('catena-hash', require('./components/catena-hash.vue'));
Vue.component('catena-identity', require('./components/catena-identity.vue'));
Vue.component('catena-identities', require('./components/catena-identities.vue'));
Vue.component('catena-payload', require('./components/catena-payload.vue'));
Vue.component('catena-query', require('./components/catena-query.vue'));
Vue.component('catena-spinner', require('./components/catena-spinner.vue'));
Vue.component('catena-tabs', require('./components/catena-tabs.vue'));
Vue.component('catena-tab', require('./components/catena-tab.vue'));
Vue.component('catena-timestamp', require('./components/catena-timestamp.vue'));
Vue.component('catena-transaction', require('./components/catena-transaction.vue'));
Vue.component('catena-raw-transaction', require('./components/catena-raw-transaction.vue'));

const messages = {
	en: {
		app: {
			name: "Catena"
		}
	},

	nl: {
		app: {
			name: "Catena"
		}
	}
};

let lang = (Vue.config.lang || window.navigator.userLanguage || window.navigator.language || "en").substr(0,2);

var app = new Vue({
	el: '#app',
	i18n: new VueInt({
		locale: lang,
		fallbackLocale: 'en', 
		messages: messages,
		silentTranslationWarn: true
	}),
	render: h => h(App)
});