#ifndef ETCD_ERROR_CODES_H
#define ETCD_ERROR_CODES_H

/**
 * We need to check error codes embedded inside Exception classes
 * thrown by the etcd library
 * 
 * Added this file to enumerate the codes instead of embedding
 * numbers inside files
 *
 * for version 2, 
 * see https://github.com/coreos/etcd/blob/master/Documentation/v2/errorcode.md
 * for version 3 extensions
 * https://github.com/coreos/etcd/blob/master/error/error.go
 */

namespace etcd {


enum ErrorCode {
	EcodeKeyNotFound = 100,
	EcodeTestFailed,
	EcodeNotFile,
	EcodeNotDir,
	EcodeNodeExist,
	EcodeRootROnly,
	EcodeDirNotEmpty,
	EcodeExistingPeerAddr,
	EcodeUnauthorized,

	EcodePrevValueRequired = 200,
	EcodeTTLNaN,
	EcodeIndexNaN,

	EcodeInvalidField = 209,
	EcodeInvalidForm,
	EcodeRefreshValue,
	EcodeRefreshTTLRequired,

	EcodeRaftInternal = 300,
	EcodeLeaderElect,

	EcodeWatcherCleared = 400,
	EcodeEventIndexCleared,
};

} // namespace

#endif
