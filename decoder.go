// Copyright (c) 2020 MindStand Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package gogm

import (
	"errors"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"reflect"
	"strings"
)

func decode(result neo4j.Result, respObj interface{}) (err error) {
	var rows [][]interface{}

	numRows := 0
	for result.Next() {
		rows = append(rows, result.Record().Values())
		numRows++
	}

	if numRows == 0 {
		return ErrNotFound
	}
	return innerDecode(rows, respObj)
}

//decodes raw path response from driver
//example query `match p=(n)-[*0..5]-() return p`
func innerDecode(result [][]interface{}, respObj interface{}) (err error) {
	//check nil params
	if result == nil {
		return fmt.Errorf("result can not be nil, %w", ErrInvalidParams)
	}

	//we're doing reflection now, lets set up a panic recovery
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v - PANIC RECOVERY - %w", r, ErrInternal)
		}
	}()

	if respObj == nil {
		return fmt.Errorf("response object can not be nil - %w", ErrInvalidParams)
	}

	rv := reflect.ValueOf(respObj)
	rt := reflect.TypeOf(respObj)

	primaryLabel := getPrimaryLabel(rt)

	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("invalid resp type %T - %w", respObj, ErrInvalidParams)
	}

	//todo optimize with set array size
	var paths []neo4j.Path
	var strictRels []neo4j.Relationship
	var isolatedNodes []neo4j.Node

	for _, row := range result {
		for _, graphType := range row {
			switch graphType.(type) {
			case neo4j.Path:
				paths = append(paths, graphType.(neo4j.Path))
				break
			case neo4j.Relationship:
				strictRels = append(strictRels, graphType.(neo4j.Relationship))
				break
			case neo4j.Node:
				isolatedNodes = append(isolatedNodes, graphType.(neo4j.Node))
				break
			default:
				continue
			}
		}
	}
	nodeLookup := make(map[int64]*reflect.Value)
	relMaps := make(map[int64]map[string]*RelationConfig)
	var pks []int64
	rels := make(map[int64]*neoEdgeConfig)
	labelLookup := map[int64]string{}

	if paths != nil && len(paths) != 0 {
		err = sortPaths(paths, &nodeLookup, &rels, &pks, primaryLabel, &relMaps)
		if err != nil {
			return err
		}
	}

	if isolatedNodes != nil && len(isolatedNodes) != 0 {
		err = sortIsolatedNodes(isolatedNodes, &labelLookup, &nodeLookup, &pks, primaryLabel, &relMaps)
		if err != nil {
			return err
		}
	}

	if strictRels != nil && len(strictRels) != 0 {
		err = sortStrictRels(strictRels, &labelLookup, &rels)
		if err != nil {
			return err
		}
	}

	//check if we have anything to do
	if len(pks) == 0 {
		return fmt.Errorf("no primary nodes to return, %w", ErrNotFound)
	}

	//build relationships
	for _, relationConfig := range rels {
		if relationConfig.StartNodeType == "" || relationConfig.EndNodeType == "" {
			continue
		}

		//grab reflect value for start
		start, _, err := getValueAndConfig(relationConfig.StartNodeId, relationConfig.StartNodeType, nodeLookup)
		if err != nil {
			return err
		}

		//grab reflect value for end
		end, _, err := getValueAndConfig(relationConfig.EndNodeId, relationConfig.EndNodeType, nodeLookup)
		if err != nil {
			return err
		}

		startConfig, endConfig, err := mappedRelations.GetConfigs(relationConfig.StartNodeType, relationConfig.EndNodeType,
			relationConfig.EndNodeType, relationConfig.StartNodeType, relationConfig.Type)
		if err != nil {
			return err
		}

		// handle from start side of edge
		if startMap, ok := relMaps[relationConfig.StartNodeId]; ok {
			if conf, ok := startMap[startConfig.FieldName]; ok {
				conf.Ids = append(conf.Ids, relationConfig.EndNodeId)
			} else {
				var rt RelationType
				if startConfig.ManyRelationship {
					rt = Multi
				} else {
					rt = Single
				}

				newConf := &RelationConfig{
					Ids:          []int64{relationConfig.EndNodeId},
					RelationType: rt,
				}

				startMap[startConfig.FieldName] = newConf
			}
		} else {
			return fmt.Errorf("relation config not found for id [%v]", relationConfig.StartNodeId)
		}

		// handle from end side of edge
		if endMap, ok := relMaps[relationConfig.EndNodeId]; ok {
			if conf, ok := endMap[endConfig.FieldName]; ok {
				conf.Ids = append(conf.Ids, relationConfig.StartNodeId)
			} else {
				var rt RelationType
				if endConfig.ManyRelationship {
					rt = Multi
				} else {
					rt = Single
				}

				newConf := &RelationConfig{
					Ids:          []int64{relationConfig.StartNodeId},
					RelationType: rt,
				}

				endMap[endConfig.FieldName] = newConf
			}
		} else {
			return fmt.Errorf("relation config not found for id [%v]", relationConfig.StartNodeId)
		}

		if startConfig.UsesEdgeNode {
			var typeConfig structDecoratorConfig

			it := startConfig.Type

			//get the actual type if its a slice
			if it.Kind() == reflect.Slice {
				it = it.Elem()
			}

			label := ""

			if it.Kind() == reflect.Ptr {
				label = it.Elem().Name()
			} else {
				label = it.Name()
				it = reflect.PtrTo(it)
			}

			temp, ok := mappedTypes.Get(label) // mappedTypes[boltNode.Labels[0]]
			if !ok {
				return fmt.Errorf("can not find mapping for node with label %s - %w", label, ErrInternal)
			}

			typeConfig = temp.(structDecoratorConfig)
			if !ok {
				return fmt.Errorf("unable to cast [%T] to structDecoratorConfig - %w", temp, ErrInternal)
			}

			//create value
			val, err := convertToValue(relationConfig.Id, typeConfig, relationConfig.Obj, it)
			if err != nil {
				return err
			}

			var startCall reflect.Value
			var endCall reflect.Value

			if start.Kind() != reflect.Ptr {
				startCall = start.Addr()
			} else {
				startCall = *start
			}

			if end.Kind() != reflect.Ptr {
				endCall = end.Addr()
			} else {
				endCall = *end
			}

			//can ensure that it implements proper interface if it made it this far
			res := val.MethodByName("SetStartNode").Call([]reflect.Value{startCall})
			if res == nil || len(res) == 0 {
				return fmt.Errorf("invalid response from edge callback - %w", err)
			} else if !res[0].IsNil() {
				return fmt.Errorf("failed call to SetStartNode - %w", res[0].Interface().(error))
			}

			res = val.MethodByName("SetEndNode").Call([]reflect.Value{endCall})
			if res == nil || len(res) == 0 {
				return fmt.Errorf("invalid response from edge callback - %w", err)
			} else if !res[0].IsNil() {
				return fmt.Errorf("failed call to SetEndNode - %w", res[0].Interface().(error))
			}

			//relate end-start
			if reflect.Indirect(*end).FieldByName(endConfig.FieldName).Kind() == reflect.Slice {
				reflect.Indirect(*end).FieldByName(endConfig.FieldName).Set(reflect.Append(reflect.Indirect(*end).FieldByName(endConfig.FieldName), *val))
			} else {
				//non slice relationships are already asserted to be pointers
				end.FieldByName(endConfig.FieldName).Set(*val)
			}

			//relate start-start
			if reflect.Indirect(*start).FieldByName(startConfig.FieldName).Kind() == reflect.Slice {
				reflect.Indirect(*start).FieldByName(startConfig.FieldName).Set(reflect.Append(reflect.Indirect(*start).FieldByName(startConfig.FieldName), *val))
			} else {
				start.FieldByName(startConfig.FieldName).Set(*val)
			}
		} else {
			if end.FieldByName(endConfig.FieldName).Kind() == reflect.Slice {
				end.FieldByName(endConfig.FieldName).Set(reflect.Append(end.FieldByName(endConfig.FieldName), start.Addr()))
			} else {
				end.FieldByName(endConfig.FieldName).Set(start.Addr())
			}

			//relate end-start
			if start.FieldByName(startConfig.FieldName).Kind() == reflect.Slice {
				start.FieldByName(startConfig.FieldName).Set(reflect.Append(start.FieldByName(startConfig.FieldName), end.Addr()))
			} else {
				start.FieldByName(startConfig.FieldName).Set(end.Addr())
			}
		}
	}

	//set load maps
	if len(rels) != 0 {
		for id, val := range nodeLookup {
			reflect.Indirect(*val).FieldByName(loadMapField).Set(reflect.ValueOf(relMaps[id]))
		}
	}

	//handle if its returning a slice -- validation has been done at an earlier step
	if rt.Elem().Kind() == reflect.Slice {

		reflection := reflect.MakeSlice(rt.Elem(), 0, cap(pks))

		reflectionValue := reflect.New(reflection.Type())
		reflectionValue.Elem().Set(reflection)

		slicePtr := reflect.ValueOf(reflectionValue.Interface())

		sliceValuePtr := slicePtr.Elem()

		sliceType := rt.Elem().Elem()

		for _, id := range pks {
			val, ok := nodeLookup[id]
			if !ok {
				return fmt.Errorf("cannot find value with id (%v)", id)
			}

			//handle slice of pointers
			if sliceType.Kind() == reflect.Ptr {
				sliceValuePtr.Set(reflect.Append(sliceValuePtr, val.Addr()))
			} else {
				sliceValuePtr.Set(reflect.Append(sliceValuePtr, *val))
			}
		}

		reflect.Indirect(rv).Set(sliceValuePtr)

		return err
	} else {
		//handles single -- already checked to make sure p2 is at least 1
		reflect.Indirect(rv).Set(*nodeLookup[pks[0]])

		return err
	}
}

// getPrimaryLabel gets the label from a reflect type
func getPrimaryLabel(rt reflect.Type) string {
	//assume its already a pointer
	rt = rt.Elem()

	if rt.Kind() == reflect.Slice {
		rt = rt.Elem()
		if rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}
	}

	return rt.Name()
}

// sortIsolatedNodes process nodes that are returned individually from bolt driver
func sortIsolatedNodes(isolatedNodes []neo4j.Node, labelLookup *map[int64]string, nodeLookup *map[int64]*reflect.Value, pks *[]int64, pkLabel string, relMaps *map[int64]map[string]*RelationConfig) error {
	if isolatedNodes == nil {
		return fmt.Errorf("isolatedNodes can not be nil, %w", ErrInternal)
	}

	for _, node := range isolatedNodes {
		if node == nil {
			return fmt.Errorf("node should not be nil, %w", ErrInternal)
		}

		//check if node has already been found by another process
		if _, ok := (*nodeLookup)[node.Id()]; !ok {
			//if it hasn't, map it
			val, err := convertNodeToValue(node)
			if err != nil {
				return err
			}

			(*nodeLookup)[node.Id()] = val
			(*relMaps)[node.Id()] = map[string]*RelationConfig{}

			//primary to return
			if node.Labels() != nil && len(node.Labels()) != 0 && node.Labels()[0] == pkLabel {
				*pks = append(*pks, node.Id())
			}

			//set label map
			if _, ok := (*labelLookup)[node.Id()]; !ok && len(node.Labels()) != 0 && node.Labels()[0] == pkLabel {
				(*labelLookup)[node.Id()] = node.Labels()[0]
			}
		}
	}

	return nil
}

// sortStrictRels sorts relationships that are strictly defined (i.e direction is pre defined) from the bolt driver
func sortStrictRels(strictRels []neo4j.Relationship, labelLookup *map[int64]string, rels *map[int64]*neoEdgeConfig) error {
	if strictRels == nil {
		return fmt.Errorf("paths is empty, that shouldn't have happened, %w", ErrInternal)
	}

	for _, rel := range strictRels {
		if rel == nil {
			return errors.New("path can not be nil")
		}

		if _, ok := (*rels)[rel.Id()]; !ok {
			startLabel, ok := (*labelLookup)[rel.StartId()]
			if !ok {
				return fmt.Errorf("label not found for node [%v], %w", rel.Id(), ErrInternal)
			}

			endLabel, ok := (*labelLookup)[rel.EndId()]
			if !ok {
				return fmt.Errorf("label not found for node [%v], %w", rel.EndId(), ErrInternal)
			}

			(*rels)[rel.Id()] = &neoEdgeConfig{
				Id:            rel.Id(),
				StartNodeId:   rel.StartId(),
				StartNodeType: startLabel,
				EndNodeId:     rel.EndId(),
				EndNodeType:   endLabel,
				Obj:           rel.Props(),
				Type:          rel.Type(),
			}
		}
	}

	return nil
}

// sortPaths sorts nodes and relationships from bolt driver that dont specify the direction explicitly, instead uses the bolt spec to determine direction
func sortPaths(paths []neo4j.Path, nodeLookup *map[int64]*reflect.Value, rels *map[int64]*neoEdgeConfig, pks *[]int64, pkLabel string, relMaps *map[int64]map[string]*RelationConfig) error {
	if paths == nil {
		return fmt.Errorf("paths is empty, that shouldn't have happened, %w", ErrInternal)
	}

	for _, path := range paths {
		if path == nil {
			return errors.New("path can not be nil")
		}

		if path.Nodes() == nil || len(path.Nodes()) == 0 {
			return fmt.Errorf("no nodes found, %w", ErrNotFound)
		}

		labelLookup := make(map[int64]string, len(path.Nodes()))

		for _, node := range path.Nodes() {
			if _, ok := labelLookup[node.Id()]; !ok && len(node.Labels()) != 0 {
				labelLookup[node.Id()] = node.Labels()[0]
			}
			if _, ok := (*nodeLookup)[node.Id()]; !ok {
				//we haven't parsed this one yet, lets do that now
				val, err := convertNodeToValue(node)
				if err != nil {
					return err
				}

				(*nodeLookup)[node.Id()] = val
				(*relMaps)[node.Id()] = map[string]*RelationConfig{}

				//primary to return
				if node.Labels() != nil && len(node.Labels()) != 0 && node.Labels()[0] == pkLabel {
					*pks = append(*pks, node.Id())
				}
			}
		}

		for _, rel := range path.Relationships() {
			startLabel, ok := labelLookup[rel.StartId()]
			if !ok {
				return fmt.Errorf("label not found for node with graphId [%v], %w", rel.StartId(), ErrInternal)
			}

			endLabel, ok := labelLookup[rel.EndId()]
			if !ok {
				return fmt.Errorf("label not found for node with graphId [%v], %w", rel.EndId(), ErrInternal)
			}

			if _, ok := (*rels)[rel.Id()]; !ok {
				(*rels)[rel.Id()] = &neoEdgeConfig{
					Id:            rel.Id(),
					StartNodeId:   rel.StartId(),
					StartNodeType: startLabel,
					EndNodeId:     rel.EndId(),
					EndNodeType:   endLabel,
					Obj:           rel.Props(),
					Type:          rel.Type(),
				}
			}
		}
	}

	return nil
}

// getValueAndConfig returns reflect value of specific node and the configuration for the node
func getValueAndConfig(id int64, t string, nodeLookup map[int64]*reflect.Value) (val *reflect.Value, conf structDecoratorConfig, err error) {
	var ok bool

	val, ok = nodeLookup[id]
	if !ok {
		return nil, structDecoratorConfig{}, fmt.Errorf("value for id (%v) not found", id)
	}

	temp, ok := mappedTypes.Get(t)
	if !ok {
		return nil, structDecoratorConfig{}, fmt.Errorf("no config found for type (%s)", t)
	}

	conf, ok = temp.(structDecoratorConfig)
	if !ok {
		return nil, structDecoratorConfig{}, errors.New("unable to cast to structDecoratorConfig")
	}

	return
}

var sliceOfEmptyInterface []interface{}
var emptyInterfaceType = reflect.TypeOf(sliceOfEmptyInterface).Elem()

// convertToValue converts properties map from neo4j to golang reflect value
func convertToValue(graphId int64, conf structDecoratorConfig, props map[string]interface{}, rtype reflect.Type) (valss *reflect.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	if rtype == nil {
		return nil, errors.New("rtype can not be nil")
	}

	isPtr := false
	if rtype.Kind() == reflect.Ptr {
		isPtr = true
		rtype = rtype.Elem()
	}

	val := reflect.New(rtype)

	if graphId >= 0 {
		reflect.Indirect(val).FieldByName("Id").Set(reflect.ValueOf(graphId))
	}

	for field, fieldConfig := range conf.Fields {
		if fieldConfig.Name == "id" {
			continue //id is handled above
		}

		//skip if its a relation field
		if fieldConfig.Relationship != "" {
			continue
		}

		if fieldConfig.Ignore {
			continue
		}

		if fieldConfig.Properties {
			if fieldConfig.PropConfig == nil {
				return nil, errors.New("property config is nil for property field")
			}
			if fieldConfig.PropConfig.IsMap {
				for k, v := range props {
					if !strings.Contains(k, fieldConfig.Name) {
						//not one of our map fields
						continue
					}

					var sub reflect.Type
					if fieldConfig.PropConfig.IsMapSlice {
						sub = reflect.SliceOf(fieldConfig.PropConfig.SubType)
					} else {
						sub = fieldConfig.PropConfig.SubType
					}
					mapType := reflect.MapOf(reflect.TypeOf(""), sub)
					mapVal := reflect.MakeMap(mapType)

					mapKey := strings.Replace(k, fieldConfig.Name+".", "", 1)
					mapVal.SetMapIndex(reflect.ValueOf(mapKey), reflect.ValueOf(v))
					reflect.Indirect(val).FieldByName(field).Set(mapVal)
				}
				continue
			}
		}

		var raw interface{}
		var ok bool

		raw, ok = props[fieldConfig.Name]
		if !ok {
			if fieldConfig.IsTypeDef {
				log.Debugf("skipping field %s since it is typedeffed and not defined", fieldConfig.Name)
				continue
			}
		}

		rawVal := reflect.ValueOf(raw)

		if raw == nil || rawVal.IsZero() {
			continue //its already initialized to 0 value, no need to do anything
		} else {
			indirect := reflect.Indirect(val)
			if indirect.FieldByName(field).Type() == rawVal.Type() {
				indirect.FieldByName(field).Set(rawVal)
			} else {
				indirect.FieldByName(field).Set(rawVal.Convert(indirect.FieldByName(field).Type()))
			}
		}
	}

	//if its not a pointer, dereference it
	if !isPtr {
		retV := reflect.Indirect(val)
		return &retV, nil
	}

	return &val, err
}

// convertNodeToValue converts raw bolt node to reflect value
func convertNodeToValue(boltNode neo4j.Node) (*reflect.Value, error) {

	if boltNode.Labels() == nil || len(boltNode.Labels()) == 0 {
		return nil, errors.New("boltNode has no labels")
	}

	var typeConfig structDecoratorConfig

	temp, ok := mappedTypes.Get(boltNode.Labels()[0]) // mappedTypes[boltNode.Labels[0]]
	if !ok {
		return nil, fmt.Errorf("can not find mapping for node with label %s", boltNode.Labels()[0])
	}

	typeConfig, ok = temp.(structDecoratorConfig)
	if !ok {
		return nil, errors.New("unable to cast to struct decorator config")
	}

	return convertToValue(boltNode.Id(), typeConfig, boltNode.Props(), typeConfig.Type)
}
