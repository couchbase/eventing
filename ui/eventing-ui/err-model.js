// Base template for error messages.
function ErrorMessage(version, revision, error, details) {
    this.version = version;
    this.revision = revision;
    this.error = error;
    this.details = details;
}

ErrorMessage.prototype.hasRetry = function(errorCode) {
    return $.inArray('retry', this.error[errorCode].attrs) !== -1;
};

// Provides error handling capabilities according to attributes of errors.
function ErrorHandler(errCodes) {
    ErrorHandler.headerKey = errCodes.header_key;
    this.version = errCodes.version;
    this.revision = errCodes.revision;
    this.errors = {};

    for (var error of errCodes.errors) {
        this.errors[error.code] = {
            name: error.name,
            description: error.description,
            attributes: error.attributes
        };
    }
}
ErrorHandler.prototype.getErrorName = function(errCode) {
    if (errCode in this.errors) {
        return this.errors[errCode];
    }

    throw `${errCode} not received as part of handshake`;
};

// Creates and returns an ErrorMessage instance.
ErrorHandler.prototype.createErrorMsg = function(errCode, details) {
    console.assert(this.errors[errCode], 'Error code ' + errCode + ' not defined');

    var error = {};
    error[errCode] = {
        name: this.errors[errCode].name,
        desc: this.errors[errCode].description,
        attrs: this.errors[errCode].attributes
    };

    if (details) {
        error.details = details;
    }

    return new ErrorMessage(this.version, this.revision, error, details);
};

// Resolves an error as per its attribute.
ErrorHandler.prototype.tryResolve = function(promiseFunc, promiseCallback, errorMsg, errorCode) {
    if (errorMsg.hasRetry(errorCode)) {
        function retry() {
            promiseFunc()
                .then(function(response) {
                    var responseCode = Number(response.headers(ErrorHandler.headerKey));
                    if (!responseCode) {
                        promiseCallback(response);
                        return;
                    }

                    setTimeout(retry, 1000);
                })
                .catch(function(errResponse) {
                    console.error('Will not retry further', errResponse);
                });
        }

        retry();
    }
};