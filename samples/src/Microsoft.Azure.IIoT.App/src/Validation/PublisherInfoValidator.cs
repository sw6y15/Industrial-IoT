// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.App.Validation {
    using FluentValidation;
    using Microsoft.Azure.IIoT.App.Models;
    using Microsoft.Azure.IIoT.OpcUa.Api.Registry.Models;
    using System;

    public class PublisherInfoValidator : AbstractValidator<PublisherInfoRequested> {

        private static readonly ValidationUtils kUtils = new ValidationUtils();

        public PublisherInfoValidator() {
            RuleFor(p => p.RequestedLogLevel)
                .Must(BePositiveInteger)
                .WithMessage("Bad Log level value.");
        }

        private bool BePositiveInteger(string value) {
            if (kUtils.ShouldUseDefaultValue(value)) {
                return true;
            }

            if (Enum.TryParse(value, out TraceLogLevel _)) {
                return true;
            }

            return false;
        }
    }
}
