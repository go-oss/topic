package topic

import (
	"testing"

	"github.com/golang/mock/gomock"
)

const randomStrForTest = "random_str"

func init() {
	randomString = func() string {
		return randomStrForTest
	}
}

func TestCondAnd(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target",
			).Return(
				// output
				"topic:target",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target",
			),
		)

		cond := source{"source"}.And(source{"target"})
		cond.Exec(mockPipeline)
	}
}

func TestCondAndInAnd(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target_a",
				"topic:target_b",
			),
		)

		cond := source{"source"}.
			And(
				And(source{"target_a"}, source{"target_b"}),
			)
		cond.Exec(mockPipeline)
	}
}

func TestCondAndMulti(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target_a",
				"topic:target_b",
			),
		)

		cond := source{"source"}.
			And(source{"target_a"}, source{"target_b"})
		cond.Exec(mockPipeline)
	}
}

func TestCondAndInOr(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"topic:target_a",
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Union(
				// input
				"sess:random_str",
				"topic:source",
				"sess:random_str",
			),
		)

		cond := source{"source"}.
			Or(
				And(source{"target_a"}, source{"target_b"}),
			)
		cond.Exec(mockPipeline)
	}
}

func TestCondAndOr(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Union(
				// input
				"sess:random_str",
				"sess:random_str",
				"topic:target_b",
			),
		)

		cond := source{"source"}.And(source{"target_a"}).Or(source{"target_b"})
		cond.Exec(mockPipeline)
	}
}

func TestCondOr(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target",
			).Return(
				// output
				"topic:target",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Union(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target",
			),
		)

		cond := source{"source"}.Or(source{"target"})
		cond.Exec(mockPipeline)
	}
}

func TestCondOrInOr(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Union(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target_a",
				"topic:target_b",
			),
		)

		cond := source{"source"}.
			Or(
				Or(source{"target_a"}, source{"target_b"}),
			)
		cond.Exec(mockPipeline)
	}
}

func TestCondOrMulti(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Union(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target_a",
				"topic:target_b",
			),
		)

		cond := source{"source"}.
			Or(source{"target_a"}, source{"target_b"})
		cond.Exec(mockPipeline)
	}
}

func TestCondOrInAnd(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Union(
				// input
				"sess:random_str",
				"topic:target_a",
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"topic:source",
				"sess:random_str",
			),
		)

		cond := source{"source"}.
			And(
				Or(source{"target_a"}, source{"target_b"}),
			)
		cond.Exec(mockPipeline)
	}
}

func TestCondOrAnd(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Union(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"sess:random_str",
				"topic:target_b",
			),
		)

		cond := source{"source"}.Or(source{"target_a"}).And(source{"target_b"})
		cond.Exec(mockPipeline)
	}
}

func TestCondNot(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target",
			).Return(
				// output
				"topic:target",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Diff(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target",
			),
		)

		cond := source{"source"}.Not(source{"target"})
		cond.Exec(mockPipeline)
	}
}

func TestCondNotInNot(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Diff(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target_a",
				"topic:target_b",
			),
		)

		cond := source{"source"}.
			Not(
				Not(source{"target_a"}, source{"target_b"}),
			)
		cond.Exec(mockPipeline)
	}
}

func TestCondNotMulti(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Diff(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target_a",
				"topic:target_b",
			),
		)

		cond := source{"source"}.
			Not(source{"target_a"}, source{"target_b"})
		cond.Exec(mockPipeline)
	}
}

func TestCondNotInAnd(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Diff(
				// input
				"sess:random_str",
				"topic:target_a",
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"topic:source",
				"sess:random_str",
			),
		)

		// 括られた最小単位で階層が深い順に評価されます
		cond := source{"source"}.
			And(
				Not(source{"target_a"}, source{"target_b"}),
			)
		cond.Exec(mockPipeline)
	}
}

func TestCondNotAfterAnd(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_a",
			).Return(
				// output
				"topic:target_a",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"topic:source",
				"topic:target_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"target_b",
			).Return(
				// output
				"topic:target_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Diff(
				// input
				"sess:random_str",
				"sess:random_str",
				"topic:target_b",
			),
		)

		// 記述順に評価されます (演算子の優先度は同じです)
		cond := source{"source"}.
			And(source{"target_a"}).
			Not(source{"target_b"})
		cond.Exec(mockPipeline)
	}
}

func TestCondComplexPattern(t *testing.T) {
	{
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockPipeline := NewMockPipeline(ctrl)
		gomock.InOrder(
			mockPipeline.EXPECT().Source(
				// input
				"source",
			).Return(
				// output
				"topic:source",
			),
			mockPipeline.EXPECT().Source(
				// input
				"and_in_or_a",
			).Return(
				// output
				"topic:and_in_or_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"and_in_or_b",
			).Return(
				// output
				"topic:and_in_or_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"topic:and_in_or_a",
				"topic:and_in_or_b",
			),
			mockPipeline.EXPECT().Source(
				// input
				"not_in_or_after_and_a",
			).Return(
				// output
				"topic:not_in_or_after_and_a",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Diff(
				// input
				"sess:random_str",
				"sess:random_str",
				"topic:not_in_or_after_and_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"or_in_or_a",
			).Return(
				// output
				"topic:or_in_or_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"or_in_or_b",
			).Return(
				// output
				"topic:or_in_or_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Union(
				// input
				"sess:random_str",
				"topic:or_in_or_a",
				"topic:or_in_or_b",
			),
			mockPipeline.EXPECT().Source(
				// input
				"and_in_or_after_or",
			).Return(
				// output
				"topic:and_in_or_after_or",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"sess:random_str",
				"topic:and_in_or_after_or",
			),
			mockPipeline.EXPECT().Source(
				// input
				"not_in_or_after_and_b",
			).Return(
				// output
				"topic:not_in_or_after_and_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Diff(
				// input
				"sess:random_str",
				"sess:random_str",
				"topic:not_in_or_after_and_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Union(
				// input
				"sess:random_str",
				"topic:source",
				"sess:random_str",
				"sess:random_str",
			),
			mockPipeline.EXPECT().Source(
				// input
				"and_after_or_a",
			).Return(
				// output
				"topic:and_after_or_a",
			),
			mockPipeline.EXPECT().Source(
				// input
				"and_after_or_b",
			).Return(
				// output
				"topic:and_after_or_b",
			),
			mockPipeline.EXPECT().Session().Return(
				// output
				"sess:random_str",
			),
			mockPipeline.EXPECT().Inter(
				// input
				"sess:random_str",
				"sess:random_str",
				"topic:and_after_or_a",
				"topic:and_after_or_b",
			),
		)

		// 階層が深い順,記述順に評価されます (演算子の優先度は同じです)
		cond := source{"source"}.
			Or(
				And(source{"and_in_or_a"}, source{"and_in_or_b"}).
					Not(source{"not_in_or_after_and_a"}),
				Or(source{"or_in_or_a"}, source{"or_in_or_b"}).
					And(source{"and_in_or_after_or"}).
					Not(source{"not_in_or_after_and_b"}),
			).
			And(source{"and_after_or_a"}, source{"and_after_or_b"})
		cond.Exec(mockPipeline)
	}
}
