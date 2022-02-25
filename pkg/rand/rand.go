package rand

import "math/rand"

func RandomChoiceFrom(total int, required int) []int {
	choices := make([]int, total)
	for i := range choices {
		choices[i] = i
	}
	rand.Shuffle(total, func(i, j int) {
		choices[i], choices[j] = choices[j], choices[i]
	})

	return choices[:required]
}
