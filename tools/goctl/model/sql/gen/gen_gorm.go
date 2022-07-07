package gen

import (
	"bytes"
	"fmt"
	"github.com/zeromicro/go-zero/tools/goctl/config"
	"github.com/zeromicro/go-zero/tools/goctl/model/sql/model"
	"github.com/zeromicro/go-zero/tools/goctl/model/sql/parser"
	"github.com/zeromicro/go-zero/tools/goctl/model/sql/template"
	modelutil "github.com/zeromicro/go-zero/tools/goctl/model/sql/util"
	"github.com/zeromicro/go-zero/tools/goctl/util"
	"github.com/zeromicro/go-zero/tools/goctl/util/console"
	"github.com/zeromicro/go-zero/tools/goctl/util/format"
	"github.com/zeromicro/go-zero/tools/goctl/util/pathx"
	"github.com/zeromicro/go-zero/tools/goctl/util/stringx"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type GormGenerator struct {
	console.Console
	// source string
	dir          string
	pkg          string
	cfg          *config.Config
	isPostgreSql bool
	generator    *defaultGenerator
}

// NewGormGenerator creates an instance for defaultGenerator
func NewGormGenerator(dir string, cfg *config.Config, opt ...Option) (*GormGenerator, error) {
	if dir == "" {
		dir = pwd
	}
	dirAbs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	dir = dirAbs
	pkg := util.SafeString(filepath.Base(dirAbs))
	err = pathx.MkdirIfNotExist(dir)
	if err != nil {
		return nil, err
	}
	defaultGenerator, err := NewDefaultGenerator(dir, cfg)
	if err != nil {
		return nil, err
	}
	generator := &GormGenerator{dir: dir, cfg: cfg, pkg: pkg, generator: defaultGenerator}
	var optionList []Option
	optionList = append(optionList, newDefaultOption())
	optionList = append(optionList, opt...)
	for _, fn := range optionList {
		fn(generator.generator)
	}

	return generator, nil
}

func (g *GormGenerator) StartFromDDL(filename string, withCache bool, database string) error {
	err := g.GenFromDDL(filename, withCache, database)
	if err != nil {
		return err
	}
	return nil
}

func (g *GormGenerator) StartFromInformationSchema(tables map[string]*model.Table, withCache bool) error {
	m := make(map[string]*codeTuple)
	for _, each := range tables {
		table, err := parser.ConvertDataType(each)
		if err != nil {
			return err
		}

		code, err := g.genModel(*table, withCache)
		if err != nil {
			return err
		}
		customCode, err := g.genModelCustom(*table, withCache)
		if err != nil {
			return err
		}

		m[table.Name.Source()] = &codeTuple{
			modelCode:       code,
			modelCustomCode: customCode,
		}
	}

	return g.createFile(m)
}

func (g *GormGenerator) createFile(modelList map[string]*codeTuple) error {
	dirAbs, err := filepath.Abs(g.dir)
	if err != nil {
		return err
	}

	g.dir = dirAbs
	g.pkg = filepath.Base(dirAbs)
	err = pathx.MkdirIfNotExist(dirAbs)
	if err != nil {
		return err
	}

	for tableName, codes := range modelList {
		tn := stringx.From(tableName)
		modelFilename, err := format.FileNamingFormat(g.cfg.NamingFormat,
			fmt.Sprintf("%s_model", tn.Source()))
		if err != nil {
			return err
		}

		name := util.SafeString(modelFilename) + "_gen.go"
		filename := filepath.Join(dirAbs, name)
		err = ioutil.WriteFile(filename, []byte(codes.modelCode), os.ModePerm)
		if err != nil {
			return err
		}

		name = util.SafeString(modelFilename) + ".go"
		filename = filepath.Join(dirAbs, name)
		if pathx.FileExists(filename) {
			g.Warning("%s already exists, ignored.", name)
			continue
		}
		err = ioutil.WriteFile(filename, []byte(codes.modelCustomCode), os.ModePerm)
		if err != nil {
			return err
		}
	}

	// generate error file
	varFilename, err := format.FileNamingFormat(g.cfg.NamingFormat, "vars")
	if err != nil {
		return err
	}

	filename := filepath.Join(dirAbs, varFilename+".go")
	text, err := pathx.LoadTemplate(category, errTemplateFile, template.Error)
	if err != nil {
		return err
	}

	err = util.With("vars").Parse(text).SaveTo(map[string]interface{}{
		"pkg": g.pkg,
	}, filename, false)
	if err != nil {
		return err
	}

	g.Success("Done.")
	return nil
}

// ret1: key-table name,value-code
func (g *GormGenerator) genFromDDL(filename string, withCache bool, database string) (
	map[string]*codeTuple, error) {
	m := make(map[string]*codeTuple)
	tables, err := parser.Parse(filename, database)
	if err != nil {
		return nil, err
	}

	for _, e := range tables {
		code, err := g.genModel(*e, withCache)
		if err != nil {
			return nil, err
		}
		customCode, err := g.genModelCustom(*e, withCache)
		if err != nil {
			return nil, err
		}

		m[e.Name.Source()] = &codeTuple{
			modelCode:       code,
			modelCustomCode: customCode,
		}
	}

	return m, nil
}

func (g *GormGenerator) genModel(in parser.Table, withCache bool) (string, error) {
	if len(in.PrimaryKey.Name.Source()) == 0 {
		return "", fmt.Errorf("table %s: missing primary key", in.Name.Source())
	}

	primaryKey, uniqueKey := genCacheKeys(in)

	var table Table
	table.Table = in
	table.PrimaryCacheKey = primaryKey
	table.UniqueCacheKey = uniqueKey
	table.ContainsUniqueCacheKey = len(uniqueKey) > 0

	importsCode, err := genImports(table, withCache, in.ContainsTime())
	if err != nil {
		return "", err
	}

	varsCode, err := genVars(table, withCache, g.isPostgreSql)
	if err != nil {
		return "", err
	}

	insertCode, insertCodeMethod, err := genInsert(table, withCache, g.isPostgreSql)
	if err != nil {
		return "", err
	}

	findCode := make([]string, 0)
	findOneCode, findOneCodeMethod, err := genFindOne(table, withCache, g.isPostgreSql)
	if err != nil {
		return "", err
	}

	ret, err := genFindOneByField(table, withCache, g.isPostgreSql)
	if err != nil {
		return "", err
	}

	findCode = append(findCode, findOneCode, ret.findOneMethod)
	updateCode, updateCodeMethod, err := genUpdate(table, withCache, g.isPostgreSql)
	if err != nil {
		return "", err
	}

	deleteCode, deleteCodeMethod, err := genDelete(table, withCache, g.isPostgreSql)
	if err != nil {
		return "", err
	}

	var list []string
	list = append(list, insertCodeMethod, findOneCodeMethod, ret.findOneInterfaceMethod,
		updateCodeMethod, deleteCodeMethod)
	typesCode, err := genTypes(table, strings.Join(modelutil.TrimStringSlice(list), pathx.NL), withCache)
	if err != nil {
		return "", err
	}

	newCode, err := genNew(table, withCache, g.isPostgreSql)
	if err != nil {
		return "", err
	}

	tableName, err := genTableName(table)
	if err != nil {
		return "", err
	}

	code := &code{
		importsCode: importsCode,
		varsCode:    varsCode,
		typesCode:   typesCode,
		newCode:     newCode,
		insertCode:  insertCode,
		findCode:    findCode,
		updateCode:  updateCode,
		deleteCode:  deleteCode,
		cacheExtra:  ret.cacheExtra,
		tableName:   tableName,
	}

	output, err := g.executeModel(table, code)
	if err != nil {
		return "", err
	}

	return output.String(), nil
}

func (g *GormGenerator) genModelCustom(in parser.Table, withCache bool) (string, error) {
	text, err := pathx.LoadTemplate(category, modelCustomTemplateFile, template.ModelCustom)
	if err != nil {
		return "", err
	}

	t := util.With("model-custom").
		Parse(text).
		GoFmt(true)
	output, err := t.Execute(map[string]interface{}{
		"pkg":                   g.pkg,
		"withCache":             withCache,
		"upperStartCamelObject": in.Name.ToCamel(),
		"lowerStartCamelObject": stringx.From(in.Name.ToCamel()).Untitle(),
	})
	if err != nil {
		return "", err
	}

	return output.String(), nil
}

func (g *GormGenerator) executeModel(table Table, code *code) (*bytes.Buffer, error) {
	text, err := pathx.LoadTemplate(category, modelGenTemplateFile, template.ModelGen)
	if err != nil {
		return nil, err
	}
	t := util.With("model").
		Parse(text).
		GoFmt(true)
	output, err := t.Execute(map[string]interface{}{
		"pkg":         g.pkg,
		"imports":     code.importsCode,
		"vars":        code.varsCode,
		"types":       code.typesCode,
		"new":         code.newCode,
		"insert":      code.insertCode,
		"find":        strings.Join(code.findCode, "\n"),
		"update":      code.updateCode,
		"delete":      code.deleteCode,
		"extraMethod": code.cacheExtra,
		"tableName":   code.tableName,
		"data":        table,
	})
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (g *GormGenerator) GenFromDDL(filename string, withCache bool, database string) error {
	tables, err := parser.Parse(filename, database)
	if err != nil {
		return err
	}
	codeFiles := make([]*codeFile, 0)
	for _, e := range tables {
		primaryKey, uniqueKey := genCacheKeys(*e)
		var table Table
		table.Table = *e
		table.PrimaryCacheKey = primaryKey
		table.UniqueCacheKey = uniqueKey
		table.ContainsUniqueCacheKey = len(uniqueKey) > 0
		// domain
		codeFiles = append(codeFiles, g.GenDomain(table))
		// model
		codeFiles = append(codeFiles, g.GenModel(table))
		// repository
		codeFiles = append(codeFiles, g.GenRepository(table))
		// rpc dsl
		codeFiles = append(codeFiles, g.GenRpcDsl(table))
		// api dsl
		codeFiles = append(codeFiles, g.GenApiDsl(table))
	}
	// transaction
	codeFiles = append(codeFiles, g.GenTransaction())
	// migrate
	codeFiles = append(codeFiles, g.GenMigrate())
	// domain var
	codeFiles = append(codeFiles, g.GenDomainVar())

	for _, codeFileItem := range codeFiles {
		if codeFileItem == nil {
			continue
		}
		g.create(codeFileItem)
	}
	g.generator.Success("Done.")
	return nil
}

type codeFile struct {
	params       map[string]interface{}
	fileName     string
	template     string
	ignoreExist  bool
	disableGoFmt bool
}

func (g *GormGenerator) GenDomain(table Table) *codeFile {
	tmp := `
package domain

import (
	"context"
	"time"
)

type {{.upperStartCamelObject}} struct {
		{{.fields}}
}

type {{.upperStartCamelObject}}Repository interface {
	Insert(ctx context.Context, conn transaction.Conn, dm *{{.upperStartCamelObject}}) (*{{.upperStartCamelObject}}, error)
	Update(ctx context.Context, conn transaction.Conn, dm *{{.upperStartCamelObject}}) (*{{.upperStartCamelObject}}, error)
    UpdateWithVersion(ctx context.Context, conn transaction.Conn, dm *{{.upperStartCamelObject}}) (*{{.upperStartCamelObject}}, error)
	Delete(ctx context.Context, conn transaction.Conn, dm *{{.upperStartCamelObject}}) (*{{.upperStartCamelObject}}, error)
	FindOne(ctx context.Context, conn transaction.Conn, id int64) (*{{.upperStartCamelObject}}, error)
	Find(ctx context.Context, conn transaction.Conn, queryOptions map[string]interface{}) (int64, []*{{.upperStartCamelObject}}, error)
}

func (m *{{.upperStartCamelObject}}) Identify() interface{} {
	if m.Id == 0 {
		return nil
	}
	return m.Id
}
`
	fields := table.Fields
	fieldsString, err := genFieldsGorm(table, fields)
	if err != nil {
		return nil
	}
	params := map[string]interface{}{
		"fields":                fieldsString,
		"upperStartCamelObject": table.Name.ToCamel(),
	}
	return &codeFile{
		params:      params,
		fileName:    fmt.Sprintf("interanl/pkg/domain/%v.go", table.Name.ToSnake()),
		ignoreExist: true,
		template:    tmp,
	}
}

func (g *GormGenerator) GenModel(table Table) *codeFile {
	tmp := `
package models

import (
	"fmt"
	"gorm.io/gorm"
	"time"
)

type {{.upperStartCamelObject}} struct {
	{{.fields}}
}

func (m *{{.upperStartCamelObject}}) TableName() string {
	return "{{.table}}"
}

func (m *{{.upperStartCamelObject}}) BeforeCreate(tx *gorm.DB) (err error) {
	// m.CreateTime = time.Now()
	// m.UpdateTime = time.Now()
	// m.DeleteTime = time.Now()
	return
}

func (m *{{.upperStartCamelObject}}) BeforeUpdate(tx *gorm.DB) (err error) {
	// m.UpdateTime = time.Now()
	return
}

func (m *{{.upperStartCamelObject}}) CacheKeyFunc() string {
	if m.Id == 0 {
		return ""
	}
	return fmt.Sprintf("%v:cache:%v:id:%v", "project", m.TableName(), m.Id)
}

func (m *{{.upperStartCamelObject}}) CacheKeyFuncByObject(obj interface{}) string {
	if v, ok := obj.(*{{.upperStartCamelObject}}); ok {
		return v.CacheKeyFunc()
	}
	return ""
}

func (m *{{.upperStartCamelObject}}) CachePrimaryKeyFunc() string {
	if len("") == 0 {
		return ""
	}
	return fmt.Sprintf("%v:cache:%v:primarykey:%v", "project", m.TableName(), "key")
}

`
	fields := table.Fields
	fieldsString, err := genFieldsGorm(table, fields)
	if err != nil {
		return nil
	}
	params := map[string]interface{}{
		"fields":                fieldsString,
		"upperStartCamelObject": table.Name.ToCamel(),
		"table":                 table.Name.ToSnake(),
	}
	return &codeFile{
		params:      params,
		fileName:    fmt.Sprintf("interanl/pkg/db/models/%v.go", table.Name.ToSnake()),
		ignoreExist: true,
		template:    tmp,
	}
}

func (g *GormGenerator) GenRepository(table Table) *codeFile {
	tmp := `
package repository

import (
	"context"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/tiptok/gocomm/pkg/cache"
	"gorm.io/gorm"
)

type {{.upperStartCamelObject}}Repository struct {
	*cache.CachedRepository
}

func (repository *{{.upperStartCamelObject}}Repository) Insert(ctx context.Context, conn transaction.Conn, dm *domain.{{.upperStartCamelObject}}) (*domain.{{.upperStartCamelObject}}, error) {
	var (
		err error
		m   = &models.{{.upperStartCamelObject}}{}
		tx  = conn.DB()
	)
	if m, err = repository.DomainModelToModel(dm); err != nil {
		return nil, err
	}
	if tx = tx.Model(m).Save(m); tx.Error != nil {
		return nil, tx.Error
	}
	dm.Id = m.Id
	return dm, nil

}

func (repository *{{.upperStartCamelObject}}Repository) Update(ctx context.Context, conn transaction.Conn, dm *domain.{{.upperStartCamelObject}}) (*domain.{{.upperStartCamelObject}}, error) {
	var (
		err error
		m   *models.{{.upperStartCamelObject}}
		tx  = conn.DB()
	)
	if m, err = repository.DomainModelToModel(dm); err != nil {
		return nil, err
	}
	queryFunc := func() (interface{}, error) {
		tx = tx.Model(m).Updates(m)
		return nil, tx.Error
	}
	if _, err = repository.Query(queryFunc, m.CacheKeyFunc()); err != nil {
		return nil, err
	}
	return dm, nil
}

func (repository *{{.upperStartCamelObject}}Repository) UpdateWithVersion(ctx context.Context, transaction transaction.Conn, dm *domain.{{.upperStartCamelObject}}) (*domain.{{.upperStartCamelObject}}, error) {
	var (
		err error
		m   *models.{{.upperStartCamelObject}}
		tx  = transaction.DB()
	)
	if m, err = repository.DomainModelToModel(dm); err != nil {
		return nil, err
	}
	oldVersion := dm.Version
	m.Version += 1
	queryFunc := func() (interface{}, error) {
		tx = tx.Model(m).Where("id = ?", m.Id).Where("version = ?", oldVersion).Updates(m)
		return nil, tx.Error
	}
	if _, err = repository.Query(queryFunc, m.CacheKeyFunc()); err != nil {
		return nil, err
	}
	return dm, nil
}

func (repository *{{.upperStartCamelObject}}Repository) Delete(ctx context.Context, conn transaction.Conn, dm *domain.{{.upperStartCamelObject}}) (*domain.{{.upperStartCamelObject}}, error) {
	var (
		tx        = conn.DB()
		m = &models.{{.upperStartCamelObject}}{Id: dm.Identify().(int64)}
	)
	queryFunc := func() (interface{}, error) {
		tx = tx.Where("id = ?", m.Id).Delete(m)
		return m, tx.Error
	}
	if _, err := repository.Query(queryFunc, m.CacheKeyFunc()); err != nil {
		return dm, err
	}
	return dm, nil
}

func (repository *{{.upperStartCamelObject}}Repository) FindOne(ctx context.Context, conn transaction.Conn, id int64) (*domain.{{.upperStartCamelObject}}, error) {
	var (
		err error
		tx  = conn.DB()
		m   = new(models.{{.upperStartCamelObject}})
	)
	queryFunc := func() (interface{}, error) {
		tx = tx.Model(m).Where("id = ?", id).First(m)
		if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
			return nil, domain.ErrNotFound
		}
		return m, tx.Error
	}
	cacheModel := new(models.{{.upperStartCamelObject}})
	cacheModel.Id = id
	if err = repository.QueryCache(cacheModel.CacheKeyFunc, m, queryFunc); err != nil {
		return nil, err
	}
	return repository.ModelToDomainModel(m)
}

func (repository *{{.upperStartCamelObject}}Repository) Find(ctx context.Context, conn transaction.Conn, queryOptions map[string]interface{}) (int64, []*domain.{{.upperStartCamelObject}}, error) {
	var (
		tx    = conn.DB()
		ms    []*models.{{.upperStartCamelObject}}
		dms   = make([]*domain.{{.upperStartCamelObject}}, 0)
		total int64
	)
	queryFunc := func() (interface{}, error) {
		tx = tx.Model(&ms).Order("id desc")
		if total, tx = transaction.PaginationAndCount(ctx, tx, queryOptions, &ms); tx.Error != nil {
			return dms, tx.Error
		}
		return dms, nil
	}

	if _, err := repository.Query(queryFunc); err != nil {
		return 0, nil, err
	}

	for _, item := range ms {
		if dm, err := repository.ModelToDomainModel(item); err != nil {
			return 0, dms, err
		} else {
			dms = append(dms, dm)
		}
	}
	return total, dms, nil
}

func (repository *{{.upperStartCamelObject}}Repository) ModelToDomainModel(from *models.{{.upperStartCamelObject}}) (*domain.{{.upperStartCamelObject}}, error) {
	to := &domain.{{.upperStartCamelObject}}{}
	err := copier.Copy(to, from)
	return to, err
}

func (repository *{{.upperStartCamelObject}}Repository) DomainModelToModel(from *domain.{{.upperStartCamelObject}}) (*models.{{.upperStartCamelObject}}, error) {
	to := &models.{{.upperStartCamelObject}}{}
	err := copier.Copy(to, from)
	return to, err
}

func New{{.upperStartCamelObject}}Repository(cache *cache.CachedRepository) domain.{{.upperStartCamelObject}}Repository {
	return &{{.upperStartCamelObject}}Repository{CachedRepository: cache}
}
`
	fields := table.Fields
	fieldsString, err := genFields(table, fields)
	if err != nil {
		return nil
	}
	params := map[string]interface{}{
		"fields":                fieldsString,
		"upperStartCamelObject": table.Name.ToCamel(),
		"table":                 table.Name,
	}
	return &codeFile{
		params:      params,
		fileName:    fmt.Sprintf("interanl/pkg/db/repository/%v_repository.go", table.Name.ToSnake()),
		ignoreExist: true,
		template:    tmp,
	}
}

func (g *GormGenerator) GenTransaction() *codeFile {
	tmp := `
package transaction

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"sync"
)

type Context struct {
	//启用事务标识
	beginTransFlag bool
	db             *gorm.DB
	session        *gorm.DB
	lock           sync.Mutex
}

func (transactionContext *Context) Begin() error {
	transactionContext.lock.Lock()
	defer transactionContext.lock.Unlock()
	transactionContext.beginTransFlag = true
	tx := transactionContext.db.Begin()
	transactionContext.session = tx
	return nil
}

func (transactionContext *Context) Commit() error {
	transactionContext.lock.Lock()
	defer transactionContext.lock.Unlock()
	if !transactionContext.beginTransFlag {
		return nil
	}
	tx := transactionContext.session.Commit()
	return tx.Error
}

func (transactionContext *Context) Rollback() error {
	transactionContext.lock.Lock()
	defer transactionContext.lock.Unlock()
	if !transactionContext.beginTransFlag {
		return nil
	}
	tx := transactionContext.session.Rollback()
	return tx.Error
}

func (transactionContext *Context) DB() *gorm.DB {
	if transactionContext.beginTransFlag && transactionContext.session != nil {
		return transactionContext.session
	}
	return transactionContext.db
}

func NewTransactionContext(db *gorm.DB) *Context {
	return &Context{
		db: db,
	}
}

type Conn interface {
	Begin() error
	Commit() error
	Rollback() error
	DB() *gorm.DB
}

// UseTrans when beginTrans is true , it will begin a new transaction
// to execute the function, recover when  panic happen
func UseTrans(ctx context.Context,
	db *gorm.DB,
	fn func(context.Context, Conn) error, beginTrans bool) (err error) {
	var tx Conn
	tx = NewTransactionContext(db)
	if beginTrans {
		if err = tx.Begin(); err != nil {
			return
		}
	}
	defer func() {
		if p := recover(); p != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("recover from %#v, rollback failed: %w", p, e)
			} else {
				err = fmt.Errorf("recoveer from %#v", p)
			}
		} else if err != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("transaction failed: %s, rollback failed: %w", err, e)
			}
		} else {
			err = tx.Commit()
		}
	}()

	return fn(ctx, tx)
}

func PaginationAndCount(ctx context.Context, tx *gorm.DB, params map[string]interface{}, v interface{}) (int64, *gorm.DB) {
	var total int64
	var enableCounter bool = true
	if v, ok := params["enableCounter"]; ok {
		enableCounter = v.(bool)
	}
	if enableCounter {
		tx = tx.Count(&total)
		if tx.Error != nil {
			return total, tx
		}
	}
	if v, ok := params["offset"]; ok {
		tx.Offset(v.(int))
	}
	if v, ok := params["limit"]; ok {
		tx.Limit(v.(int))
	}
	if tx = tx.Find(v); tx.Error != nil {
		return 0, tx
	}
	return total, tx
}
`
	params := map[string]interface{}{}
	return &codeFile{
		params:      params,
		fileName:    fmt.Sprintf("interanl/pkg/db/transaction/%v.go", "transaction"),
		ignoreExist: true,
		template:    tmp,
	}
}

func (g *GormGenerator) GenMigrate() *codeFile {
	tmp := `
package db

import (
	"gorm.io/gorm"
)

func Migrate(db *gorm.DB) {
	db.AutoMigrate()
}
`
	//fields := table.Fields
	//fieldsString, err := genFields(table, fields)
	//if err != nil {
	//	return nil
	//}
	params := map[string]interface{}{
		//"fields":                fieldsString,
		//"upperStartCamelObject": table.Name.ToCamel(),
		//"table":                 table.Name,
	}
	return &codeFile{
		params:      params,
		fileName:    fmt.Sprintf("interanl/pkg/db/%v.go", "migrate"),
		ignoreExist: true,
		template:    tmp,
	}
}

func (g *GormGenerator) GenDomainVar() *codeFile {
	tmp := `
package domain

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var ErrNotFound = sqlx.ErrNotFound
`
	params := map[string]interface{}{}
	return &codeFile{
		params:      params,
		fileName:    fmt.Sprintf("interanl/pkg/domain/%v.go", "vars"),
		ignoreExist: true,
		template:    tmp,
	}
}

func (g *GormGenerator) GenRpcDsl(table Table) *codeFile {
	tmp := `
syntax = "proto3";

option go_package ="./pb";

package pb;

message {{.upperStartCamelObject}}GetReq {
   int64 Id = 1;
}
message {{.upperStartCamelObject}}GetResp{
    {{.upperStartCamelObject}}Item User = 1;
}

message {{.upperStartCamelObject}}SaveReq {

}
message {{.upperStartCamelObject}}SaveResp{

}

message {{.upperStartCamelObject}}DeleteReq {
  int64 Id = 1;
}
message {{.upperStartCamelObject}}DeleteResp{

}

message {{.upperStartCamelObject}}UpdateReq {
  int64 Id = 1;
}
message {{.upperStartCamelObject}}UpdateResp{

}

message {{.upperStartCamelObject}}SearchReq {
  int64 PageNumber = 1;
  int64 PageSize = 2;
}
message {{.upperStartCamelObject}}SearchResp{
  repeated {{.upperStartCamelObject}}Item List =1;
  int64  Total =2;
}
message {{.upperStartCamelObject}}Item {

}

service {{.upperStartCamelObject}}Service {
  rpc {{.upperStartCamelObject}}Get({{.upperStartCamelObject}}GetReq) returns({{.upperStartCamelObject}}GetResp);
  rpc {{.upperStartCamelObject}}Save({{.upperStartCamelObject}}SaveReq) returns({{.upperStartCamelObject}}SaveResp);
  rpc {{.upperStartCamelObject}}Delete({{.upperStartCamelObject}}DeleteReq) returns({{.upperStartCamelObject}}DeleteResp);
  rpc {{.upperStartCamelObject}}Update({{.upperStartCamelObject}}UpdateReq) returns({{.upperStartCamelObject}}UpdateResp);
  rpc {{.upperStartCamelObject}}Search({{.upperStartCamelObject}}SearchReq) returns({{.upperStartCamelObject}}SearchResp);
}
`
	params := map[string]interface{}{
		//"fields":                fieldsString,
		"upperStartCamelObject": table.Name.ToCamel(),
		//"table":                 table.Name,
	}
	return &codeFile{
		params:       params,
		fileName:     fmt.Sprintf("doc/dsl/rpc/%v.proto", table.Name.ToSnake()),
		ignoreExist:  true,
		template:     tmp,
		disableGoFmt: true,
	}
}

func (g *GormGenerator) GenApiDsl(table Table) *codeFile {
	tmp := `
syntax = "v1"

info(
    title: "xx实例"
    desc: "xx实例"
    author: "author"
    email: "email"
    version: "v1"
)

@server(
    prefix: {{.lowerStartCamelObject}}/v1
    group: {{.lowerStartCamelObject}}
    jwt: JwtAuth
)
service {{.lowerStartCamelObject}} {
    @handler {{.unTitleObject}}Get
    post /{{.lowerStartCamelObject}}/:id ({{.upperStartCamelObject}}GetReq) returns ({{.upperStartCamelObject}}GetResp)
    @handler {{.unTitleObject}}Save
    post /{{.lowerStartCamelObject}} ({{.upperStartCamelObject}}SaveReq) returns ({{.upperStartCamelObject}}SaveResp)
    @handler {{.unTitleObject}}Delete
    delete /{{.lowerStartCamelObject}}/:id ({{.upperStartCamelObject}}DeleteReq) returns ({{.upperStartCamelObject}}DeleteResp)
    @handler {{.unTitleObject}}Update
    put /{{.lowerStartCamelObject}}/:id ({{.upperStartCamelObject}}UpdateReq) returns ({{.upperStartCamelObject}}UpdateResp)
    @handler {{.unTitleObject}}Search
    post /{{.lowerStartCamelObject}}/search ({{.upperStartCamelObject}}SearchReq) returns ({{.upperStartCamelObject}}SearchResp)
}

type (
    {{.upperStartCamelObject}}GetReq {
        Id int64
    }
    {{.upperStartCamelObject}}GetResp {

    }
)

type  (
    {{.upperStartCamelObject}}SaveReq struct{

    }
    {{.upperStartCamelObject}}SaveResp struct{}
)

type(
    {{.upperStartCamelObject}}DeleteReq struct{
        Id int64
    }
    {{.upperStartCamelObject}}DeleteResp struct{}
)

type(
    {{.upperStartCamelObject}}UpdateReq struct{

    }
    {{.upperStartCamelObject}}UpdateResp{}
)

type(
    {{.upperStartCamelObject}}SearchReq {
         PageNumber int64
         PageSize int64
    }
    {{.upperStartCamelObject}}SearchResp{
        List []{{.upperStartCamelObject}}Item
        Total int64
    }
)

type  {{.upperStartCamelObject}}Item {

}
`
	params := map[string]interface{}{
		//"fields":                fieldsString,
		"upperStartCamelObject": table.Name.ToCamel(),
		"lowerStartCamelObject": table.Name.Lower(),
		"unTitleObject":         table.Name.Untitle(),
		//"table":                 table.Name,
	}
	return &codeFile{
		params:       params,
		fileName:     fmt.Sprintf("doc/dsl/api/%v.api", table.Name.ToSnake()),
		ignoreExist:  true,
		template:     tmp,
		disableGoFmt: true,
	}
}

func (g *GormGenerator) create(codeFileItem *codeFile) error {
	t := util.With("model").
		Parse(codeFileItem.template).
		GoFmt(true)
	if codeFileItem.disableGoFmt {
		t.GoFmt(false)
	}
	output, err := t.Execute(codeFileItem.params)
	if err != nil {
		return err
	}

	dirAbs, err := filepath.Abs(g.dir)
	if err != nil {
		return err
	}

	g.dir = dirAbs
	g.pkg = filepath.Base(dirAbs)
	err = pathx.MkdirIfNotExist(dirAbs)
	if err != nil {
		return err
	}

	filename := filepath.Join(dirAbs, codeFileItem.fileName)
	if pathx.FileExists(filename) {
		g.generator.Warning("%s already exists, ignored.", codeFileItem.fileName)
		return nil
	}
	baseDir := filepath.Dir(filename)
	if err != nil {
		return err
	}
	err = pathx.MkdirIfNotExist(baseDir)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filename, output.Bytes(), os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func genFieldsGorm(table Table, fields []*parser.Field) (string, error) {
	var list []string

	for _, field := range fields {
		result, err := genFieldGorm(table, field)
		if err != nil {
			return "", err
		}

		list = append(list, result)
	}

	return strings.Join(list, "\n"), nil
}

func genFieldGorm(table Table, field *parser.Field) (string, error) {
	tag := ""

	text, err := pathx.LoadTemplate(category, fieldTemplateFile, template.Field)
	if err != nil {
		return "", err
	}

	output, err := util.With("types").
		Parse(text).
		Execute(map[string]interface{}{
			"name":       util.SafeString(field.Name.ToCamel()),
			"type":       field.DataType,
			"tag":        tag,
			"hasComment": field.Comment != "",
			"comment":    field.Comment,
			"data":       table,
		})
	if err != nil {
		return "", err
	}

	return output.String(), nil
}
