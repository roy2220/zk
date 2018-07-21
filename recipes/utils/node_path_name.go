package utils

func GetNodeParentPath(nodePath string) string {
	var i int

	for i = len(nodePath) - 1; i >= 0; i-- {
		if nodePath[i] == '/' {
			break
		}
	}

	return nodePath[:i]
}

func GetNodeName(nodePath string) string {
	var i int

	for i = len(nodePath) - 1; i >= 0; i-- {
		if nodePath[i] == '/' {
			break
		}
	}

	return nodePath[i+1:]
}
