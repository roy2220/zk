package utils

const SequenceNumberDelimiter = ':'

type SequentialNodeNames []string

func (self SequentialNodeNames) Len() int {
	return len(self)
}

func (self SequentialNodeNames) Less(i int, j int) bool {
	a := self[i]
	var ii int

	for ii = len(a) - 1; ii >= 0; ii-- {
		if a[ii] == SequenceNumberDelimiter {
			break
		}
	}

	b := self[j]
	var jj int

	for jj = len(b) - 1; jj >= 0; jj-- {
		if b[jj] == SequenceNumberDelimiter {
			break
		}
	}

	return a[ii+1:] < b[jj+1:]
}

func (self SequentialNodeNames) Swap(i int, j int) {
	self[i], self[j] = self[j], self[i]
}
