// Copyright (c) 2022 MindStand Technologies, Inc
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
	"reflect"
	"strconv"

	dsl "github.com/mindstand/go-cypherdsl"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// nodeCreate holds configuration for creating new nodes
type nodeCreate struct {
	// params to save
	Params map[string]interface{}
	// type to save by
	Type reflect.Type
	// Id
	Id interface{}
	// Pointer value (if id is not yet set)
	Pointer uintptr
	// whether the node is new or not
	IsNew bool
	// whether the node is for a patch update
	IsPatch bool
	// the PrimaryKeyStrategy for this node type
	PKS *PrimaryKeyStrategy
}

// relCreate holds configuration for nodes to link together
type relCreate struct {
	// start uuid of relationship
	StartNodePtr uintptr
	// end uuid of relationship
	EndNodePtr uintptr
	// any data to store in edge
	Params map[string]interface{}
	// holds direction of the edge
	Direction dsl.Direction
}

func saveDepth(gogm *Gogm, obj interface{}, depth int) neo4j.TransactionWork {
	return func(tx neo4j.Transaction) (interface{}, error) {
		if obj == nil {
			return nil, errors.New("obj can not be nil")
		}

		if depth < 0 {
			return nil, errors.New("cannot save a depth less than 0")
		}

		//validate that obj is a pointer
		rawType := reflect.TypeOf(obj)
		var derefType reflect.Type

		if rawType.Kind() == reflect.Ptr {
			//validate that the dereference type is a struct
			derefType = rawType.Elem()

			if derefType.Kind() != reflect.Struct {
				return nil, fmt.Errorf("dereference type can not be of type %T", obj)
			}
		} else if rawType.Kind() == reflect.Slice {
			//validate that the dereference type is a pointer to struct
			tmpDT := rawType.Elem()

			if tmpDT.Kind() != reflect.Ptr {
				return nil, fmt.Errorf("dereference type of slice can not be of type %T", obj)
			}
			//validate that the dereference type is a pointer to struct
			derefType = tmpDT.Elem()

			if derefType.Kind() != reflect.Struct {
				return nil, fmt.Errorf("dereference type can not be of type %T", obj)
			}
		} else {
			return nil, fmt.Errorf("obj must be of type pointer or slice of pointers, not %T", obj)
		}

		var (
			// [LABEL][int64 (graphid) or uintptr]{config}
			nodes = map[string]map[uintptr]*nodeCreate{}
			// [LABEL] []{config}
			relations = map[string][]*relCreate{}
			// node id -- [field] config
			oldRels = map[uintptr]map[string]*RelationConfig{}
			// node id -- [field] config
			curRels = map[interface{}]map[string]*RelationConfig{}
			// id to reflect value
			nodeIdRef = map[uintptr]interface{}{}
			// uintptr to reflect value (for new nodes that dont have a graph id yet)
			nodeRef = map[uintptr]*reflect.Value{}
		)

		rootVal := reflect.ValueOf(obj)
		err := parseStruct(gogm, 0, "", false, dsl.DirectionBoth, nil, &rootVal, 0, depth,
			nodes, relations, nodeIdRef, nodeRef, oldRels)
		if err != nil {
			return nil, fmt.Errorf("failed to parse struct, %w", err)
		}
		// save/update nodes
		err = createNodes(tx, gogm, nodes, nodeRef, nodeIdRef)
		if err != nil {
			return nil, fmt.Errorf("failed to create nodes, %w", err)
		}

		// generate rel maps
		err = generateCurRels(gogm, 0, &rootVal, 0, depth, curRels)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate current relationships, %w", err)
		}

		dels, err := calculateDels(oldRels, curRels, nodeIdRef)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate relationships to delete, %w", err)
		}

		//fix the cur rels and write them to their perspective nodes
		for ptr, val := range nodeRef {
			graphId, ok := nodeIdRef[ptr]
			if !ok {
				return nil, fmt.Errorf("graph id for node ptr [%v] not found", ptr)
			}
			loadConf, ok := curRels[graphId]
			if !ok {
				return nil, fmt.Errorf("load config not found for node [%v]", graphId)
			}

			//handle if its a pointer
			if val.Kind() == reflect.Ptr {
				*val = val.Elem()
			}

			reflect.Indirect(*val).FieldByName("LoadMap").Set(reflect.ValueOf(loadConf))
		}

		if len(dels) != 0 {
			err := removeRelations(tx, dels)
			if err != nil {
				return nil, err
			}
		}

		if len(relations) != 0 {
			err := relateNodes(tx, relations, nodeIdRef, nodeRef)
			if err != nil {
				return nil, err
			}
		}

		return obj, nil
	}
}

func getNodeId(nodeRef map[uintptr]*reflect.Value, lookup map[uintptr]interface{}, nodePtr uintptr) (interface{}, error) {
	tmpId, ok := lookup[nodePtr]
	if !ok {
		return nil, fmt.Errorf("graph id not found for ptr %v", nodePtr)
	}

	var actualId interface{}
	var ok2 bool

	actualId, ok2 = tmpId.(string)
	if ok2 {
		// For nodes with string keys, we try to find the internal neo4j int64 id
		nodeVal := nodeRef[nodePtr]
		pkVal := nodeVal.FieldByName(DefaultPrimaryKeyStrategy.FieldName)

		if pkVal.IsNil() {
			return nil, fmt.Errorf("default graph id not found for ptr: %v id: %v", nodePtr, actualId)
		}
		if !pkVal.Elem().IsZero() {
			id := pkVal.Elem().Int()
			return id, nil
		}
	} else {
		actualId = tmpId
	}
	return actualId, nil
}

// relateNodes connects nodes together using edge config
func relateNodes(transaction neo4j.Transaction, relations map[string][]*relCreate,
	lookup map[uintptr]interface{}, nodeRef map[uintptr]*reflect.Value) error {
	if len(relations) == 0 {
		return errors.New("relations can not be nil or empty")
	}

	for label, rels := range relations {
		var _params []interface{}

		if len(rels) == 0 {
			continue
		}

		for _, rel := range rels {
			// grab start id
			startId, err := getNodeId(nodeRef, lookup, rel.StartNodePtr)
			if err != nil {
				return fmt.Errorf("graph id not found for ptr %v", rel.StartNodePtr)
			}

			endId, err := getNodeId(nodeRef, lookup, rel.EndNodePtr)
			if err != nil {
				return fmt.Errorf("graph id not found for ptr %v", rel.EndNodePtr)
			}

			//set map if its empty
			if rel.Params == nil {
				rel.Params = map[string]interface{}{}
			}

			_params = append(_params, map[string]interface{}{
				"startNodeId": startId,
				"endNodeId":   endId,
				"props":       rel.Params,
			})
		}

		mergePath, err := dsl.Path().
			V(dsl.V{
				Name: "startNode",
			}).
			E(dsl.E{
				Name: "rel",
				Types: []string{
					label,
				},
				Direction: dsl.DirectionOutgoing,
			}).
			V(dsl.V{
				Name: "endNode",
			}).
			ToCypher()
		if err != nil {
			return err
		}

		cyp, err := dsl.QB().
			Cypher("UNWIND $rows as row").
			Match(dsl.Path().V(dsl.V{Name: "startNode"}).Build()).
			Where(dsl.C(&dsl.ConditionConfig{
				FieldManipulationFunction: "ID",
				Name:                      "startNode",
				ConditionOperator:         dsl.EqualToOperator,
				Check:                     dsl.ParamString("row.startNodeId"),
			})).
			With(&dsl.WithConfig{
				Parts: []dsl.WithPart{
					{
						Name: "row",
					},
					{
						Name: "startNode",
					},
				},
			}).
			Match(dsl.Path().V(dsl.V{Name: "endNode"}).Build()).
			Where(dsl.C(&dsl.ConditionConfig{
				FieldManipulationFunction: "ID",
				Name:                      "endNode",
				ConditionOperator:         dsl.EqualToOperator,
				Check:                     dsl.ParamString("row.endNodeId"),
			})).
			Merge(&dsl.MergeConfig{
				Path: mergePath,
			}).
			Cypher("SET rel += row.props").
			ToCypher()
		if err != nil {
			return fmt.Errorf("failed to build query, %w", err)
		}

		res, err := transaction.Run(cyp, map[string]interface{}{
			"rows": _params,
		})
		if err != nil {
			return fmt.Errorf("failed to relate nodes, %w", err)
		} else if err = res.Err(); err != nil {
			return fmt.Errorf("failed to relate nodes %w", res.Err())
		}
	}

	return nil
}

// removes relationships between specified nodes
func removeRelations(transaction neo4j.Transaction, dels map[interface{}][]interface{}) error {
	if len(dels) == 0 {
		return nil
	}

	var params []interface{}

	expectedDels := 0
	for id, ids := range dels {
		params = append(params, map[string]interface{}{
			"startNodeId": id,
			"endNodeIds":  ids,
		})
		expectedDels += len(ids)
	}

	cyq, err := dsl.QB().
		Cypher("UNWIND $rows as row").
		Match(dsl.Path().
			V(dsl.V{
				Name: "start",
			}).E(dsl.E{
			Name:      "e",
			Direction: dsl.DirectionNone,
		}).V(dsl.V{
			Name: "end",
		}).Build()).
		Cypher("WHERE id(start) = row.startNodeId and id(end) in row.endNodeIds").
		Delete(false, "e").
		ToCypher()
	if err != nil {
		return err
	}

	res, err := transaction.Run(cyq, map[string]interface{}{
		"rows": params,
	})
	if err != nil {
		return fmt.Errorf("%s: %w", err.Error(), ErrInternal)
	} else if err = res.Err(); err != nil {
		return fmt.Errorf("%s: %w", err.Error(), ErrInternal)
	}

	summary, err := res.Consume()
	if err != nil {
		return fmt.Errorf("failed to consume result summary, %s: %w", err.Error(), ErrInternal)
	}

	actualRelsDeleted := summary.Counters().RelationshipsDeleted()
	if expectedDels != actualRelsDeleted {
		return fmt.Errorf("expected relationship deletions not equal to actual. Expected=%v|Actual=%v", expectedDels, actualRelsDeleted)
	}

	return nil
}

// calculates which relationships to delete
func calculateDels(oldRels map[uintptr]map[string]*RelationConfig, curRels map[interface{}]map[string]*RelationConfig, lookup map[uintptr]interface{}) (map[interface{}][]interface{}, error) {
	if len(oldRels) == 0 {
		return map[interface{}][]interface{}{}, nil
	}

	dels := map[interface{}][]interface{}{}

	for ptr, oldRelConf := range oldRels {
		oldId, ok := lookup[ptr]
		if !ok {
			return nil, fmt.Errorf("graph id not found for ptr [%v]", ptr)
		}
		curRelConf, ok := curRels[oldId]
		deleteAllRels := false
		if !ok {
			//this means that the node is gone, remove all rels to this node
			deleteAllRels = true
		} else {
			for field, oldConf := range oldRelConf {
				curConf, ok := curRelConf[field]
				deleteAllRelsOnField := false
				if !ok {
					//this means that either the field has been removed or there are no more rels on this field,
					//either way delete anything left over
					deleteAllRelsOnField = true
				}
				for _, id := range oldConf.Ids {
					//check if this id is new rels in the same location
					if deleteAllRels || deleteAllRelsOnField {
						if _, ok := dels[oldId]; !ok {
							dels[oldId] = []interface{}{id}
						} else {
							dels[oldId] = append(dels[oldId], id)
						}
					} else {
						if !interfaceSliceContains(curConf.Ids, id) {
							if _, ok := dels[oldId]; !ok {
								dels[oldId] = []interface{}{id}
							} else {
								dels[oldId] = append(dels[oldId], id)
							}
						}
					}
				}
			}
		}
	}

	return dels, nil
}

func generateCurRels(gogm *Gogm, parentPtr uintptr, current *reflect.Value, currentDepth, maxDepth int, curRels map[interface{}]map[string]*RelationConfig) error {
	t := reflect.TypeOf(current.Interface())

	if t.Kind() == reflect.Slice {
		i := 0
		for i < current.Len() {
			c := current.Index(i)
			e := generateCurRelsInternal(gogm, parentPtr, &c, currentDepth, maxDepth, curRels)
			if e != nil {
				return e
			}
			i += 1
		}

		return nil
	}

	return generateCurRelsInternal(gogm, parentPtr, current, currentDepth, maxDepth, curRels)
}

func generateCurRelsInternal(gogm *Gogm, parentPtr uintptr, current *reflect.Value, currentDepth, maxDepth int, curRels map[interface{}]map[string]*RelationConfig) error {
	if currentDepth > maxDepth {
		return nil
	}

	curPtr := current.Pointer()

	// check for going in circles
	if parentPtr == curPtr {
		return nil
	}

	//get the type
	nodeType, _, err := getTypeName(current.Type())
	if err != nil {
		return err
	}

	// Get the PKS given the nodeType
	pks := gogm.GetPrimaryKeyStrategy(nodeType)

	var idVal reflect.Value
	var id interface{}

	if pks.FieldName != DefaultPrimaryKeyStrategy.FieldName {
		idVal = reflect.Indirect(*current).FieldByName(pks.FieldName)
		if idVal.IsZero() {
			return errors.New("string id not set")
		}
		id = idVal.String()
	} else {
		idVal := reflect.Indirect(*current).FieldByName(DefaultPrimaryKeyStrategy.FieldName)
		if idVal.IsNil() {
			return errors.New("id not set")
		}

		if !idVal.Elem().IsZero() {
			id = idVal.Elem().Int()
		} else {
			id = 0
		}
	}

	if _, ok := curRels[id]; ok {
		//this node has already been seen
		return nil
	} else {
		//create the record for it
		curRels[id] = map[string]*RelationConfig{}
	}

	//get the config
	actual, ok := gogm.mappedTypes.Get(nodeType)
	if !ok {
		return fmt.Errorf("struct config not found type (%s)", nodeType)
	}

	//cast the config
	currentConf, ok := actual.(structDecoratorConfig)
	if !ok {
		return errors.New("unable to cast into struct decorator config")
	}
	for _, conf := range currentConf.Fields {
		if conf.Relationship == "" {
			continue
		}

		relField := reflect.Indirect(*current).FieldByName(conf.FieldName)

		//if its nil, just skip it
		if relField.IsNil() {
			continue
		}

		if conf.ManyRelationship {
			slLen := relField.Len()
			if slLen == 0 {
				continue
			}

			for i := 0; i < slLen; i++ {
				relVal := relField.Index(i)

				newParentId, _, _, _, _, followVal, err := processStruct(gogm, conf, &relVal, curPtr)
				if err != nil {
					return err
				}

				var followId interface{}
				e1 := followVal.Elem()

				//get the type
				nodeType2, _, err2 := getTypeName(followVal.Type())
				if err2 != nil {
					return err2
				}

				// Get the PKS given the nodeType
				pks2 := gogm.GetPrimaryKeyStrategy(nodeType2)

				followIdVal := e1.FieldByName(pks2.FieldName)
				if pks2.FieldName != DefaultPrimaryKeyStrategy.FieldName {
					if !followIdVal.IsZero() {
						followId = followIdVal.String()
					} else {
						// should not be nil, just skip this one
						continue
					}
				} else {
					if !followIdVal.IsNil() {
						followIdVal = followIdVal.Elem()
						followId = followIdVal.Int()
					} else {
						// should not be nil, just skip this one
						continue
					}
				}

				//check the config is there for the specified field
				if _, ok = curRels[id][conf.FieldName]; !ok {
					curRels[id][conf.FieldName] = &RelationConfig{
						Ids:          []interface{}{},
						RelationType: Multi,
					}
				}

				curRels[id][conf.FieldName].Ids = append(curRels[id][conf.FieldName].Ids, followId)

				if followVal.Pointer() != parentPtr {
					err = generateCurRels(gogm, newParentId, followVal, currentDepth+1, maxDepth, curRels)
					if err != nil {
						return err
					}
				}
			}
		} else {
			newParentId, _, _, _, _, followVal, err := processStruct(gogm, conf, &relField, curPtr)
			if err != nil {
				return err
			}

			var followId interface{}
			e1 := followVal.Elem()

			//get the type
			nodeType2, _, err2 := getTypeName(followVal.Type())
			if err2 != nil {
				return err2
			}

			// Get the PKS given the nodeType
			pks2 := gogm.GetPrimaryKeyStrategy(nodeType2)

			followIdVal := e1.FieldByName(pks2.FieldName)
			if pks2.FieldName != DefaultPrimaryKeyStrategy.FieldName {
				if !followIdVal.IsZero() {
					followId = followIdVal.String()
				} else {
					// should not be nil, just skip this one
					continue
				}
			} else {
				if !followIdVal.IsNil() {
					followIdVal = followIdVal.Elem()
					followId = followIdVal.Int()
				} else {
					// should not be nil, just skip this one
					continue
				}
			}

			//check the config is there for the specified field
			if _, ok = curRels[id][conf.FieldName]; !ok {
				curRels[id][conf.FieldName] = &RelationConfig{
					Ids:          []interface{}{},
					RelationType: Single,
				}
			}

			curRels[id][conf.FieldName].Ids = append(curRels[id][conf.FieldName].Ids, followId)

			if followVal.Pointer() != parentPtr {
				err = generateCurRels(gogm, newParentId, followVal, currentDepth+1, maxDepth, curRels)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// createNodes updates existing nodes and creates new nodes while also making a lookup table for ptr -> neoid
func createNodes(transaction neo4j.Transaction, gogm *Gogm, crNodes map[string]map[uintptr]*nodeCreate, nodeRef map[uintptr]*reflect.Value, nodeIdRef map[uintptr]interface{}) error {
	for label, nodes := range crNodes {
		// used when the id of the node hasn't been set yet
		var i uint64 = 0
		var nodesArr []*nodeCreate

		var updateRows, newRows []interface{}
		for ptr, config := range nodes {
			row := map[string]interface{}{}

			if config.IsPatch && config.PKS.StrategyName != DefaultPrimaryKeyStrategy.StrategyName {
				// This is an update for the non-default PrimaryKeyStrategy
				row[config.PKS.DBName] = config.Params[config.PKS.DBName]
				delete(config.Params, config.PKS.DBName)
				row["obj"] = config.Params
				row["key_field"] = config.PKS.DBName
				row["i"] = fmt.Sprintf("%d", i)
				updateRows = append(updateRows, row)
			} else if id, ok := nodeIdRef[ptr]; ok {
				var intId int64
				var strId string
				if intId, ok = id.(int64); ok {
					row["obj"] = config.Params
					row["key_field"] = "id"
					row["id"] = intId
				} else if strId, ok = id.(string); ok {
					row[config.PKS.DBName] = strId
					delete(config.Params, config.PKS.DBName)
					row["obj"] = config.Params
					row["key_field"] = config.PKS.DBName
					row["i"] = fmt.Sprintf("%d", i)
				}
				updateRows = append(updateRows, row)
			} else {
				if config.PKS.StrategyName != DefaultPrimaryKeyStrategy.StrategyName {
					row[config.PKS.DBName] = config.Params[config.PKS.DBName]
					row["key_field"] = config.PKS.DBName
				}
				row["obj"] = config.Params
				row["i"] = fmt.Sprintf("%d", i)
				newRows = append(newRows, row)
			}

			nodesArr = append(nodesArr, config)
			i++
		}

		// create new stuff
		if len(newRows) != 0 {
			pks := gogm.GetPrimaryKeyStrategy(label)
			var newC dsl.Cypher

			if pks.FieldName == DefaultPrimaryKeyStrategy.FieldName {
				newC = dsl.QB().
					Cypher("UNWIND $rows as row").
					Cypher(fmt.Sprintf("CREATE(n:`%s`)", label)).
					Cypher("SET n += row.obj").
					Return(false, dsl.ReturnPart{
						Name:  "row.i",
						Alias: "i",
					}, dsl.ReturnPart{
						Function: &dsl.FunctionConfig{
							Name:   "ID",
							Params: []interface{}{dsl.ParamString("n")},
						},
						Alias: "id",
					})
			} else {
				// Make sure that we update the existing node with the given string key
				newC = dsl.QB().
					Cypher("UNWIND $rows as row").
					Cypher(fmt.Sprintf("MERGE(n:`%s`{%s:row.%s})", label, pks.DBName, pks.DBName)).
					Cypher("SET n += row.obj").
					Return(false, dsl.ReturnPart{
						Name:  "row.i",
						Alias: "i",
					}, dsl.ReturnPart{
						Function: &dsl.FunctionConfig{
							Name:   "ID",
							Params: []interface{}{dsl.ParamString("n")},
						},
						Alias: "id",
					}, dsl.ReturnPart{
						Name:  fmt.Sprintf("row.%s", pks.DBName),
						Alias: "unique_id",
					})
			}

			cyp, err := newC.
				ToCypher()
			if err != nil {
				return fmt.Errorf("failed to build query, %w", err)
			}

			res, err := transaction.Run(cyp, map[string]interface{}{
				"rows": newRows,
			})
			if err != nil {
				return fmt.Errorf("failed to execute new node query, %w", err)
			} else if res.Err() != nil {
				return fmt.Errorf("failed to execute new node query from result error, %w", res.Err())
			}

			for res.Next() {
				row := res.Record().Values
				if len(row) != 2 && len(row) != 3 {
					continue
				}

				strPtr, ok := row[0].(string)
				if !ok {
					return fmt.Errorf("cannot cast row[0] to string, %w", ErrInternal)
				}

				i, err = strconv.ParseUint(strPtr, 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse i string to uint64, %w", err)
				}

				if i > uint64(len(nodesArr)) {
					return fmt.Errorf("returned node index %d is outside of node array bounds", i)
				}

				graphId, ok := row[1].(int64)
				if !ok {
					return fmt.Errorf("cannot cast row[1] to int64, %w", ErrInternal)
				}

				// get node reference from index
				ptr := nodesArr[i].Pointer

				// get the node val
				val, ok := nodeRef[ptr]
				if !ok {
					return fmt.Errorf("cannot find val for ptr [%d]", ptr)
				}

				if len(row) == 3 {
					// we only support default (int64) or string type of primary keys
					strGraphId, ok2 := row[2].(string)
					if !ok2 {
						return fmt.Errorf("cannot cast row[2] to string, %w", ErrInternal)
					}
					// update the lookup with the string index
					nodeIdRef[ptr] = strGraphId

					// set the new string id
					reflect.Indirect(*val).FieldByName(pks.FieldName).Set(reflect.ValueOf(strGraphId))
					// set the new id
					reflect.Indirect(*val).FieldByName(DefaultPrimaryKeyStrategy.FieldName).Set(reflect.ValueOf(&graphId))
				} else {
					// update the lookup
					nodeIdRef[ptr] = graphId

					// set the new id
					reflect.Indirect(*val).FieldByName(DefaultPrimaryKeyStrategy.FieldName).Set(reflect.ValueOf(&graphId))
				}
			}
		}

		// process stuff that we're updating
		if len(updateRows) != 0 {
			pks := gogm.GetPrimaryKeyStrategy(label)
			path, err := dsl.Path().V(dsl.V{
				Name: "n",
				Type: "`" + label + "`",
			}).ToCypher()
			if err != nil {
				return err
			}

			var whereClause string = ""
			var addReturn bool = false

			if pks.FieldName == DefaultPrimaryKeyStrategy.FieldName {
				whereClause = "WHERE ID(n) = row.id"
			} else {
				whereClause = fmt.Sprintf("WHERE n.%s = row.%s", pks.DBName, pks.DBName)
				addReturn = true
			}

			newC := dsl.QB().
				Cypher("UNWIND $rows as row").
				Cypher(fmt.Sprintf("MATCH %s", path)).
				Cypher(whereClause).
				Cypher("SET n += row.obj")

			// we need data back in case this label type uses non default PKS
			// We get the neo4j ID back and set it in `nodeIdRef`
			if addReturn {
				newC = newC.Return(false, dsl.ReturnPart{
					Name:  "row.i",
					Alias: "i",
				}, dsl.ReturnPart{
					Function: &dsl.FunctionConfig{
						Name:   "ID",
						Params: []interface{}{dsl.ParamString("n")},
					},
					Alias: "id",
				}, dsl.ReturnPart{
					Name:  fmt.Sprintf("row.%s", pks.DBName),
					Alias: "unique_id",
				})
			} else {
				newC = newC.Return(false, dsl.ReturnPart{
					Name:  "row.i",
					Alias: "i",
				}, dsl.ReturnPart{
					Function: &dsl.FunctionConfig{
						Name:   "ID",
						Params: []interface{}{dsl.ParamString("n")},
					},
					Alias: "id",
				})
			}

			cyp, err := newC.ToCypher()
			if err != nil {
				return fmt.Errorf("failed to build query, %w", err)
			}

			res, err := transaction.Run(cyp, map[string]interface{}{
				"rows": updateRows,
			})
			if err != nil {
				return fmt.Errorf("failed to run update query, %w", err)
			} else if res.Err() != nil {
				return fmt.Errorf("failed to run update query, %w", res.Err())
			}

			if addReturn {
				for res.Next() {
					row := res.Record().Values
					if len(row) != 2 && len(row) != 3 {
						continue
					}

					strPtr, ok := row[0].(string)
					if !ok {
						return fmt.Errorf("cannot cast row[0] to string, %w", ErrInternal)
					}

					i, err = strconv.ParseUint(strPtr, 10, 64)
					if err != nil {
						return fmt.Errorf("failed to parse i string to uint64, %w", err)
					}

					if i > uint64(len(nodesArr)) {
						return fmt.Errorf("returned node index %d is outside of node array bounds", i)
					}

					graphId, ok := row[1].(int64)
					if !ok {
						return fmt.Errorf("cannot cast row[1] to int64, %w", ErrInternal)
					}

					// get node reference from index
					ptr := nodesArr[i].Pointer

					// get the node val
					val, ok := nodeRef[ptr]
					if !ok {
						return fmt.Errorf("cannot find val for ptr [%d]", ptr)
					}

					if len(row) == 3 {
						// we only support default (int64) or string type of primary keys
						strGraphId, ok2 := row[2].(string)
						if !ok2 {
							return fmt.Errorf("cannot cast row[2] to string, %w", ErrInternal)
						}
						// update the lookup with the string index
						nodeIdRef[ptr] = strGraphId

						// set the new string id
						reflect.Indirect(*val).FieldByName(pks.FieldName).Set(reflect.ValueOf(strGraphId))
						// set the new id
						reflect.Indirect(*val).FieldByName(DefaultPrimaryKeyStrategy.FieldName).Set(reflect.ValueOf(&graphId))
					} else {
						// update the lookup
						nodeIdRef[ptr] = graphId

						// set the new id
						reflect.Indirect(*val).FieldByName(DefaultPrimaryKeyStrategy.FieldName).Set(reflect.ValueOf(&graphId))
					}
				}
			}
		}
	}

	return nil
}

// parseStruct
// we are intentionally using pointers as identifiers in this stage because graph ids are not guaranteed
func parseStruct(gogm *Gogm, parentPtr uintptr, edgeLabel string, parentIsStart bool, direction dsl.Direction, edgeParams map[string]interface{}, current *reflect.Value,
	currentDepth, maxDepth int, nodes map[string]map[uintptr]*nodeCreate, relations map[string][]*relCreate, nodeIdRef map[uintptr]interface{}, nodeRef map[uintptr]*reflect.Value, oldRels map[uintptr]map[string]*RelationConfig) error {
	t := reflect.TypeOf(current.Interface())

	if t.Kind() == reflect.Slice {
		i := 0
		for i < current.Len() {
			c := current.Index(i)
			e := parseStructInternal(gogm, parentPtr, edgeLabel, parentIsStart, direction, edgeParams, &c, currentDepth,
				maxDepth, nodes, relations, nodeIdRef, nodeRef, oldRels)
			if e != nil {
				return e
			}
			i += 1
		}

		return nil
	}

	return parseStructInternal(gogm, parentPtr, edgeLabel, parentIsStart, direction, edgeParams, current, currentDepth,
		maxDepth, nodes, relations, nodeIdRef, nodeRef, oldRels)
}

// parseStruct
// we are intentionally using pointers as identifiers in this stage because graph ids are not guaranteed
func parseStructInternal(gogm *Gogm, parentPtr uintptr, edgeLabel string, parentIsStart bool, direction dsl.Direction, edgeParams map[string]interface{}, current *reflect.Value,
	currentDepth, maxDepth int, nodes map[string]map[uintptr]*nodeCreate, relations map[string][]*relCreate, nodeIdRef map[uintptr]interface{}, nodeRef map[uintptr]*reflect.Value, oldRels map[uintptr]map[string]*RelationConfig) error {
	//check if its done
	if currentDepth > maxDepth {
		return nil
	}

	curPtr := current.Pointer()

	//get the type
	nodeType, isPatch, err := getTypeName(current.Type())
	if err != nil {
		return err
	}

	// get the config
	actual, ok := gogm.mappedTypes.Get(nodeType)
	if !ok {
		return fmt.Errorf("struct config not found type (%s)", nodeType)
	}

	//cast the config
	currentConf, ok := actual.(structDecoratorConfig)
	if !ok {
		return errors.New("unable to cast into struct decorator config")
	}

	// Get the PKS given the nodeType
	pks := gogm.GetPrimaryKeyStrategy(nodeType)
	// grab info and set ids of current node
	isNew, graphID, relConf, err := handleNodeState(pks, isPatch, current)
	if err != nil {
		return fmt.Errorf("failed to handle node, %w", err)
	}

	// handle edge
	if parentPtr != 0 {
		if _, ok := relations[edgeLabel]; !ok {
			relations[edgeLabel] = []*relCreate{}
		}

		var start, end uintptr
		curDir := direction

		if parentIsStart {
			start = parentPtr
			end = curPtr
		} else {
			start = curPtr
			end = parentPtr
			if curDir == dsl.DirectionIncoming {
				curDir = dsl.DirectionOutgoing
			} else if curDir == dsl.DirectionOutgoing {
				curDir = dsl.DirectionIncoming
			}
		}

		if edgeParams == nil {
			edgeParams = map[string]interface{}{}
		}

		found := false
		//check if this edge is already here
		if len(relations[edgeLabel]) != 0 {
			for _, conf := range relations[edgeLabel] {
				if conf.StartNodePtr == start && conf.EndNodePtr == end {
					found = true
				}
			}
		}

		// if not found already register the relationships
		if !found {
			relations[edgeLabel] = append(relations[edgeLabel], &relCreate{
				Direction:    curDir,
				Params:       edgeParams,
				StartNodePtr: start,
				EndNodePtr:   end,
			})
		}
	}

	if !isNew {
		intId, ok2 := graphID.(int64)
		_, ok3 := graphID.(string)
		if (ok2 && intId >= 0) || ok3 {
			if _, ok := nodeIdRef[curPtr]; !ok {
				nodeIdRef[curPtr] = graphID
			}
		}

		if _, ok := oldRels[curPtr]; !ok {
			oldRels[curPtr] = relConf
		}
	}

	// set the lookup table
	if _, ok := nodeRef[curPtr]; !ok {
		nodeRef[curPtr] = current
	}

	//convert params
	params, err := toCypherParamsMap(gogm, *current, currentConf)
	if err != nil {
		return err
	}

	//if its nil, just default it
	if params == nil {
		params = map[string]interface{}{}
	}

	//set the nodes lookup map
	if _, ok := nodes[currentConf.Label]; !ok {
		nodes[currentConf.Label] = map[uintptr]*nodeCreate{}
	}

	nodes[currentConf.Label][curPtr] = &nodeCreate{
		Params:  params,
		Type:    current.Type(),
		Id:      graphID,
		Pointer: curPtr,
		IsNew:   isNew,
		IsPatch: isPatch,
		PKS:     pks,
	}

	// loop through fields looking for edges
	for _, conf := range currentConf.Fields {
		if conf.Relationship == "" {
			// not a relationship field
			continue
		}

		relField := reflect.Indirect(*current).FieldByName(conf.FieldName)

		//if its nil, just skip it
		if relField.IsNil() {
			continue
		}

		if conf.ManyRelationship {
			slLen := relField.Len()
			if slLen == 0 {
				continue
			}

			for i := 0; i < slLen; i++ {
				relVal := relField.Index(i)
				newParentId, newEdgeLabel, newParentIsStart, newDirection, newEdgeParams, followVal, err := processStruct(gogm, conf, &relVal, curPtr)
				if err != nil {
					return err
				}

				err = parseStruct(gogm, newParentId, newEdgeLabel, newParentIsStart, newDirection, newEdgeParams, followVal, currentDepth+1, maxDepth, nodes, relations, nodeIdRef, nodeRef, oldRels)
				if err != nil {
					return err
				}
			}
		} else {
			newParentId, newEdgeLabel, newParentIsStart, newDirection, newEdgeParams, followVal, err := processStruct(gogm, conf, &relField, curPtr)
			if err != nil {
				return err
			}

			err = parseStruct(gogm, newParentId, newEdgeLabel, newParentIsStart, newDirection, newEdgeParams, followVal, currentDepth+1, maxDepth, nodes, relations, nodeIdRef, nodeRef, oldRels)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// processStruct generates configuration for individual struct for saving
func processStruct(gogm *Gogm, fieldConf decoratorConfig, relValue *reflect.Value, curPtr uintptr) (parentId uintptr, edgeLabel string, parentIsStart bool, direction dsl.Direction, edgeParams map[string]interface{}, followVal *reflect.Value, err error) {
	edgeLabel = fieldConf.Relationship

	relValName, _, err := getTypeName(relValue.Type())
	if err != nil {
		return 0, "", false, 0, nil, nil, err
	}

	actual, ok := gogm.mappedTypes.Get(relValName)
	if !ok {
		return 0, "", false, 0, nil, nil, fmt.Errorf("cannot find config for %s", edgeLabel)
	}

	edgeConf, ok := actual.(structDecoratorConfig)
	if !ok {
		return 0, "", false, 0, nil, nil, errors.New("can not cast to structDecoratorConfig")
	}

	if relValue.Type().Implements(edgeType) {
		startValSlice := relValue.MethodByName("GetStartNode").Call(nil)
		endValSlice := relValue.MethodByName("GetEndNode").Call(nil)

		if len(startValSlice) == 0 || len(endValSlice) == 0 {
			return 0, "", false, 0, nil, nil, errors.New("edge is invalid, sides are not set")
		}

		// get actual type from interface
		startVal := startValSlice[0].Elem()
		endVal := endValSlice[0].Elem()

		params, err := toCypherParamsMap(gogm, *relValue, edgeConf)
		if err != nil {
			return 0, "", false, 0, nil, nil, err
		}

		//if its nil, just default it
		if params == nil {
			params = map[string]interface{}{}
		}

		startPtr, endPtr := startVal.Pointer(), endVal.Pointer()
		if startPtr == curPtr {
			return startPtr, edgeLabel, true, fieldConf.Direction, params, &endVal, nil
		} else if endPtr == curPtr {
			return endPtr, edgeLabel, false, fieldConf.Direction, params, &startVal, nil
		} else {
			return 0, "", false, 0, nil, nil, errors.New("edge is invalid, doesn't point to parent vertex")
		}
	} else {
		return curPtr, edgeLabel, fieldConf.Direction == dsl.DirectionOutgoing, fieldConf.Direction, map[string]interface{}{}, relValue, nil
	}
}
