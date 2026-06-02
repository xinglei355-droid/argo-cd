package security

import (
	"errors"
	"fmt"

	"github.com/argoproj/argo-cd/v3/util/glob"
)

var ErrNamespaceNotPermitted = errors.New("namespace is not permitted")

func IsNamespaceNotPermittedError(err error) bool {
	return errors.Is(err, ErrNamespaceNotPermitted)
}

func IsNamespaceEnabled(namespace string, serverNamespace string, enabledNamespaces []string) bool {
	return namespace == serverNamespace || glob.MatchStringInList(enabledNamespaces, namespace, glob.REGEXP)
}

func NamespaceNotPermittedError(namespace string) error {
	return fmt.Errorf("%w: '%s'", ErrNamespaceNotPermitted, namespace)
}
