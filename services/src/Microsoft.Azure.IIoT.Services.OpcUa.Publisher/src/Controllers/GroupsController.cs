// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Services.OpcUa.Publisher.Controllers {
    using Microsoft.Azure.IIoT.Services.OpcUa.Publisher.Auth;
    using Microsoft.Azure.IIoT.Services.OpcUa.Publisher.Filters;
    using Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.Http;
    using Microsoft.Azure.IIoT.AspNetCore.OpenApi;
    using Microsoft.AspNetCore.Authorization;
    using Microsoft.AspNetCore.Mvc;
    using System.Threading.Tasks;
    using System;
    using System.Linq;

    /// <summary>
    /// CRUD and Query data set writer groups resources
    /// </summary>
    [ApiVersion("2")]
    [Route("v{version:apiVersion}/groups")]
    [ExceptionsFilter]
    [Authorize(Policy = Policies.CanRead)]
    [ApiController]
    public class GroupsController : ControllerBase {

        /// <summary>
        /// Create controller
        /// </summary>
        /// <param name="groups"></param>
        public GroupsController(IWriterGroupRegistry groups) {
            _groups = groups;
        }

        /// <summary>
        /// Adds a new writer group
        /// </summary>
        /// <remarks>
        /// Creates a new writer group and returns the assigned
        /// group identifier and generation.
        /// </remarks>
        /// <param name="request">The writer group properties</param>
        /// <returns>Assigned identifier and generation</returns>
        [HttpPut]
        [Authorize(Policy = Policies.CanWrite)]
        public async Task<WriterGroupAddResponseApiModel> CreateWriterGroupAsync(
            [FromBody] WriterGroupAddRequestApiModel request) {
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            var result = await _groups.AddWriterGroupAsync(
                request.ToServiceModel(), new PublisherOperationContextModel {
                    Time = DateTime.UtcNow,
                    AuthorityId = HttpContext.User.Identity.Name
                });
            return result.ToApiModel();
        }

        /// <summary>
        /// Get writer group
        /// </summary>
        /// <remarks>
        /// Returns a writer group with the provided identifier.
        /// </remarks>
        /// <param name="writerGroupId">The writer group identifier</param>
        /// <returns>A writer group</returns>
        [HttpGet("{writerGroupId}")]
        public async Task<WriterGroupApiModel> GetWriterGroupAsync(
            string writerGroupId) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            var group = await _groups.GetWriterGroupAsync(writerGroupId);
            return group.ToApiModel();
        }

        /// <summary>
        /// Activate writer group
        /// </summary>
        /// <remarks>
        /// Instructs the publisher to emit messages for the writer group.
        /// </remarks>
        /// <param name="writerGroupId">The writer group identifier</param>
        /// <returns>A writer group</returns>
        [HttpPost("{writerGroupId}/activate")]
        public async Task ActivateWriterGroupAsync(
            string writerGroupId) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            await _groups.ActivateWriterGroupAsync(writerGroupId,
                new PublisherOperationContextModel {
                    Time = DateTime.UtcNow,
                    AuthorityId = HttpContext.User.Identity.Name
                });
        }

        /// <summary>
        /// Dactivate writer group
        /// </summary>
        /// <remarks>
        /// Stops publishing network messages for the writer group.
        /// </remarks>
        /// <param name="writerGroupId">The writer group identifier</param>
        /// <returns>A writer group</returns>
        [HttpPost("{writerGroupId}/deactivate")]
        public async Task DeactivateWriterGroupAsync(
            string writerGroupId) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            await _groups.DeactivateWriterGroupAsync(writerGroupId,
                new PublisherOperationContextModel {
                    Time = DateTime.UtcNow,
                    AuthorityId = HttpContext.User.Identity.Name
                });
        }

        /// <summary>
        /// Updates a writer group
        /// </summary>
        /// <remarks>
        /// Patches or updates properties of a writer group. A generation
        /// identifier must be provided.  If resource is out of date error
        /// is returned patching must be retried with the current generation.
        /// </remarks>
        /// <param name="writerGroupId">The writer group identifier</param>
        /// <param name="request">Patch request</param>
        /// <returns></returns>
        [HttpPatch("{writerGroupId}")]
        [Authorize(Policy = Policies.CanWrite)]
        public async Task UpdateWriterGroupAsync(string writerGroupId,
            [FromBody] WriterGroupUpdateRequestApiModel request) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            await _groups.UpdateWriterGroupAsync(writerGroupId,
                request.ToServiceModel(), new PublisherOperationContextModel {
                    Time = DateTime.UtcNow,
                    AuthorityId = HttpContext.User.Identity.Name
                });
        }

        /// <summary>
        /// Get list of writer groups
        /// </summary>
        /// <remarks>
        /// List all data set writer groups that are registered or
        /// continues a query.
        /// </remarks>
        /// <param name="continuationToken">Optional Continuation token</param>
        /// <param name="pageSize">Optional number of results to return</param>
        /// <returns>Writer groups</returns>
        [HttpGet]
        [AutoRestExtension(NextPageLinkName = "continuationToken")]
        public async Task<WriterGroupInfoListApiModel> GetListOfWriterGroupsAsync(
            [FromQuery] string continuationToken, [FromQuery] int? pageSize) {
            if (Request.Headers.ContainsKey(HttpHeader.ContinuationToken)) {
                continuationToken = Request.Headers[HttpHeader.ContinuationToken]
                    .FirstOrDefault();
            }
            if (Request.Headers.ContainsKey(HttpHeader.MaxItemCount)) {
                pageSize = int.Parse(Request.Headers[HttpHeader.MaxItemCount]
                    .FirstOrDefault());
            }
            var result = await _groups.ListWriterGroupsAsync(
                continuationToken, pageSize);
            return result.ToApiModel();
        }

        /// <summary>
        /// Query writer groups
        /// </summary>
        /// <remarks>
        /// List writer groups that match a query model.
        /// The returned model can contain a continuation token if more results are
        /// available.
        /// Call the GetListOfWriterGroups operation using the token to retrieve
        /// more results.
        /// </remarks>
        /// <param name="query">Writer group query</param>
        /// <param name="pageSize">Optional number of results to return</param>
        /// <returns>Writer groups</returns>
        [HttpPost("query")]
        public async Task<WriterGroupInfoListApiModel> QueryWriterGroupsAsync(
            [FromBody] WriterGroupInfoQueryApiModel query, [FromQuery] int? pageSize) {
            if (query == null) {
                throw new ArgumentNullException(nameof(query));
            }
            if (Request.Headers.ContainsKey(HttpHeader.MaxItemCount)) {
                pageSize = int.Parse(Request.Headers[HttpHeader.MaxItemCount]
                    .FirstOrDefault());
            }
            var result = await _groups.QueryWriterGroupsAsync(
                query.ToServiceModel(), pageSize);

            return result.ToApiModel();
        }

        /// <summary>
        /// Removes a writer group
        /// </summary>
        /// <remarks>
        /// Removes a writer group with the specified generation identifier.
        /// If resource is out of date error is returned patching should be
        /// retried with the current generation.
        /// </remarks>
        /// <param name="writerGroupId">The writer group identifier</param>
        /// <param name="generationId">Writer group generation</param>
        /// <returns></returns>
        [HttpDelete("{writerGroupId}/{generationId}")]
        [Authorize(Policy = Policies.CanWrite)]
        public async Task DeleteWriterGroupAsync(string writerGroupId, string generationId) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            if (string.IsNullOrEmpty(generationId)) {
                throw new ArgumentNullException(nameof(generationId));
            }
            await _groups.RemoveWriterGroupAsync(writerGroupId, generationId,
                new PublisherOperationContextModel {
                    Time = DateTime.UtcNow,
                    AuthorityId = HttpContext.User.Identity.Name
                });
        }

        private readonly IWriterGroupRegistry _groups;
    }
}
