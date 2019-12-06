// Code generated by GoGM v1.0.1. DO NOT EDIT
package testing_

import (
	"errors"
)

// LinkToExampleObject2OnFieldSpecial links ExampleObject to ExampleObject2 on the fields ExampleObject.Special and ExampleObject2.Special.
// note this uses the special edge SpecialEdge
func (l *ExampleObject) LinkToExampleObject2OnFieldSpecial(target *ExampleObject2, edge *SpecialEdge) error {
	if target == nil {
		return errors.New("start and end can not be nil")
	}

	if edge == nil {
		return errors.New("edge can not be nil")
	}

	err := edge.SetStartNode(l)
	if err != nil {
		return err
	}

	err = edge.SetEndNode(target)
	if err != nil {
		return err
	}

	l.Special = edge

	if target.Special == nil {
		target.Special = make([]*SpecialEdge, 1, 1)
		target.Special[0] = edge
	} else {
		target.Special = append(target.Special, edge)
	}

	return nil
}

// UnlinkFromExampleObject2OnFieldSpecial unlinks ExampleObject from ExampleObject2 on the fields ExampleObject.Special and ExampleObject2.Special.
// also note this uses the special edge SpecialEdge
func (l *ExampleObject) UnlinkFromExampleObject2OnFieldSpecial(target *ExampleObject2) error {
	if target == nil {
		return errors.New("start and end can not be nil")
	}

	l.Special = nil

	if target.Special != nil {
		for i, unlinkTarget := range target.Special {

			obj := unlinkTarget.GetStartNode()

			checkObj, ok := obj.(*ExampleObject)
			if !ok {
				return errors.New("unable to cast unlinkTarget to [ExampleObject]")
			}
			if checkObj.UUID == l.UUID {
				a := &target.Special
				(*a)[i] = (*a)[len(*a)-1]
				(*a)[len(*a)-1] = nil
				*a = (*a)[:len(*a)-1]
				break
			}
		}
	}

	return nil
}

// LinkToExampleObjectOnFieldChildren links ExampleObject to ExampleObject on the fields ExampleObject.Children and ExampleObject.Parents
func (l *ExampleObject) LinkToExampleObjectOnFieldChildren(targets ...*ExampleObject) error {
	if targets == nil {
		return errors.New("start and end can not be nil")
	}

	for _, target := range targets {

		if l.Children == nil {
			l.Children = make([]*ExampleObject, 1, 1)
			l.Children[0] = target
		} else {
			l.Children = append(l.Children, target)
		}

		target.Parents = l
	}

	return nil
}

//UnlinkFromExampleObjectOnFieldChildren unlinks ExampleObject from ExampleObject on the fields ExampleObject.Children and ExampleObject.Parents
func (l *ExampleObject) UnlinkFromExampleObjectOnFieldChildren(targets ...*ExampleObject) error {
	if targets == nil {
		return errors.New("start and end can not be nil")
	}

	for _, target := range targets {

		if l.Children != nil {
			for i, unlinkTarget := range l.Children {
				if unlinkTarget.UUID == target.UUID {
					a := &l.Children
					(*a)[i] = (*a)[len(*a)-1]
					(*a)[len(*a)-1] = nil
					*a = (*a)[:len(*a)-1]
					break
				}
			}
		}

		target.Parents = nil
	}

	return nil
}

//LinkToExampleObjectOnFieldParents links ExampleObject to ExampleObject on the fields ExampleObject.Parents and ExampleObject.Children
func (l *ExampleObject) LinkToExampleObjectOnFieldParents(target *ExampleObject) error {
	if target == nil {
		return errors.New("start and end can not be nil")
	}

	l.Parents = target

	if target.Children == nil {
		target.Children = make([]*ExampleObject, 1, 1)
		target.Children[0] = l
	} else {
		target.Children = append(target.Children, l)
	}

	return nil
}

//UnlinkFromExampleObjectOnFieldParents unlinks ExampleObject from ExampleObject on the fields ExampleObject.Parents and ExampleObject.Children
func (l *ExampleObject) UnlinkFromExampleObjectOnFieldParents(target *ExampleObject) error {
	if target == nil {
		return errors.New("start and end can not be nil")
	}

	l.Parents = nil

	if target.Children != nil {
		for i, unlinkTarget := range target.Children {
			if unlinkTarget.UUID == l.UUID {
				a := &target.Children
				(*a)[i] = (*a)[len(*a)-1]
				(*a)[len(*a)-1] = nil
				*a = (*a)[:len(*a)-1]
				break
			}
		}
	}

	return nil
}

// LinkToExampleObjectOnFieldSpecial links ExampleObject2 to ExampleObject on the fields ExampleObject2.Special and ExampleObject.Special.
// note this uses the special edge SpecialEdge
func (l *ExampleObject2) LinkToExampleObjectOnFieldSpecial(target *ExampleObject, edge *SpecialEdge) error {
	if target == nil {
		return errors.New("start and end can not be nil")
	}

	if edge == nil {
		return errors.New("edge can not be nil")
	}

	err := edge.SetStartNode(target)
	if err != nil {
		return err
	}

	err = edge.SetEndNode(l)
	if err != nil {
		return err
	}

	if l.Special == nil {
		l.Special = make([]*SpecialEdge, 1, 1)
		l.Special[0] = edge
	} else {
		l.Special = append(l.Special, edge)
	}

	target.Special = edge

	return nil
}

// UnlinkFromExampleObjectOnFieldSpecial unlinks ExampleObject2 from ExampleObject on the fields ExampleObject2.Special and ExampleObject.Special.
// also note this uses the special edge SpecialEdge
func (l *ExampleObject2) UnlinkFromExampleObjectOnFieldSpecial(target *ExampleObject) error {
	if target == nil {
		return errors.New("start and end can not be nil")
	}

	if l.Special != nil {
		for i, unlinkTarget := range l.Special {

			obj := unlinkTarget.GetEndNode()

			checkObj, ok := obj.(*ExampleObject)
			if !ok {
				return errors.New("unable to cast unlinkTarget to [ExampleObject]")
			}
			if checkObj.UUID == target.UUID {
				a := &l.Special
				(*a)[i] = (*a)[len(*a)-1]
				(*a)[len(*a)-1] = nil
				*a = (*a)[:len(*a)-1]
				break
			}
		}
	}

	target.Special = nil

	return nil
}

// LinkToExampleObject2OnFieldChildren2 links ExampleObject2 to ExampleObject2 on the fields ExampleObject2.Children2 and ExampleObject2.Parents2
func (l *ExampleObject2) LinkToExampleObject2OnFieldChildren2(targets ...*ExampleObject2) error {
	if targets == nil {
		return errors.New("start and end can not be nil")
	}

	for _, target := range targets {

		if l.Children2 == nil {
			l.Children2 = make([]*ExampleObject2, 1, 1)
			l.Children2[0] = target
		} else {
			l.Children2 = append(l.Children2, target)
		}

		target.Parents2 = l
	}

	return nil
}

//UnlinkFromExampleObject2OnFieldChildren2 unlinks ExampleObject2 from ExampleObject2 on the fields ExampleObject2.Children2 and ExampleObject2.Parents2
func (l *ExampleObject2) UnlinkFromExampleObject2OnFieldChildren2(targets ...*ExampleObject2) error {
	if targets == nil {
		return errors.New("start and end can not be nil")
	}

	for _, target := range targets {

		if l.Children2 != nil {
			for i, unlinkTarget := range l.Children2 {
				if unlinkTarget.UUID == target.UUID {
					a := &l.Children2
					(*a)[i] = (*a)[len(*a)-1]
					(*a)[len(*a)-1] = nil
					*a = (*a)[:len(*a)-1]
					break
				}
			}
		}

		target.Parents2 = nil
	}

	return nil
}

//LinkToExampleObject2OnFieldParents2 links ExampleObject2 to ExampleObject2 on the fields ExampleObject2.Parents2 and ExampleObject2.Children2
func (l *ExampleObject2) LinkToExampleObject2OnFieldParents2(target *ExampleObject2) error {
	if target == nil {
		return errors.New("start and end can not be nil")
	}

	l.Parents2 = target

	if target.Children2 == nil {
		target.Children2 = make([]*ExampleObject2, 1, 1)
		target.Children2[0] = l
	} else {
		target.Children2 = append(target.Children2, l)
	}

	return nil
}

//UnlinkFromExampleObject2OnFieldParents2 unlinks ExampleObject2 from ExampleObject2 on the fields ExampleObject2.Parents2 and ExampleObject2.Children2
func (l *ExampleObject2) UnlinkFromExampleObject2OnFieldParents2(target *ExampleObject2) error {
	if target == nil {
		return errors.New("start and end can not be nil")
	}

	l.Parents2 = nil

	if target.Children2 != nil {
		for i, unlinkTarget := range target.Children2 {
			if unlinkTarget.UUID == l.UUID {
				a := &target.Children2
				(*a)[i] = (*a)[len(*a)-1]
				(*a)[len(*a)-1] = nil
				*a = (*a)[:len(*a)-1]
				break
			}
		}
	}

	return nil
}
