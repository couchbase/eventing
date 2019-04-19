// Config is the format required by the server
// Binding is the format required by the front-end

// Adapter provides a way to convert one form to the other
function Adapter() {}

Adapter.prototype.convertBindingsToConfig = function(bindings) {
    // A binding is of the form -
    // [{type:'', name:'', value:'', auth_type:'no-auth', allow_cookies:true, access:'r'}]
    var config = {
        buckets: [],
        curl: []
    };
    for (var binding of bindings) {
        if (binding.type === 'alias' && binding.name && binding.value) {
            config.buckets.push(this.createBucketConfig(binding));
        }
        if (binding.type === 'url' && binding.hostname && binding.value) {
            config.curl.push(this.createCurlConfig(binding));
        }
    }
    return config;
};

Adapter.prototype.convertConfigToBindings = function(config) {
    var adapter = this;
    var bindings = [];
    (config.buckets ? config.buckets : []).forEach(function(bucketConfig) {
        bindings.push(adapter.createBucketBinding(bucketConfig));
    });
    (config.curl ? config.curl : []).forEach(function(curlConfig) {
        bindings.push(adapter.createCurlBinding(curlConfig));
    });
    return bindings;
};

Adapter.prototype.createBucketConfig = function(binding) {
    var config = {};
    config[binding.type] = binding.value;
    config.bucket_name = binding.name;
    config.access = binding.access;
    return config;
};

Adapter.prototype.createBucketBinding = function(config) {
    return {
        type: 'alias',
        name: config.bucket_name,
        value: config.alias,
        access: config.access ? config.access : (config.source_bucket === config.bucket_name ? 'r' : 'rw')
    };
};

Adapter.prototype.createCurlConfig = function(binding) {
    var config = {
        hostname: binding.hostname,
        value: binding.value,
        allow_cookies: binding.allow_cookies,
        validate_ssl_certificate: binding.validate_ssl_certificate,
        auth_type: binding.auth_type
    };

    switch (binding.auth_type) {
        case 'basic':
        case 'digest':
            config.username = binding.username;
            config.password = binding.password;
            break;

        case 'bearer':
            config.bearer_key = binding.bearer_key;
            break;

        case 'no-auth':
        default:
            config.auth_type = 'no-auth';
    }
    return config;
};

Adapter.prototype.createCurlBinding = function(config) {
    var binding = {
        type: 'url',
        hostname: config.hostname,
        value: config.value,
        allow_cookies: config.allow_cookies,
        validate_ssl_certificate: config.validate_ssl_certificate,
        auth_type: config.auth_type
    };

    switch (config.auth_type) {
        case 'digest':
        case 'basic':
            binding.username = config.username;
            binding.password = config.password;
            break;

        case 'bearer':
            binding.bearer_key = config.bearer_key;
            break;

        case 'no-auth':
        default:
            binding.auth_type = 'no-auth';
    }
    return binding;
};