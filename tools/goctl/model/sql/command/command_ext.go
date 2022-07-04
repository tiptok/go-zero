package command

import (
	"errors"
	"github.com/spf13/cobra"
	"github.com/zeromicro/go-zero/tools/goctl/config"
	"github.com/zeromicro/go-zero/tools/goctl/model/sql/command/migrationnotes"
	"github.com/zeromicro/go-zero/tools/goctl/model/sql/gen"
	"github.com/zeromicro/go-zero/tools/goctl/model/sql/util"
	file "github.com/zeromicro/go-zero/tools/goctl/util"
	"github.com/zeromicro/go-zero/tools/goctl/util/console"
	"github.com/zeromicro/go-zero/tools/goctl/util/pathx"
	"strings"
)

// 扩展命令

// GormDDL generates model code from ddl
func GormDDL(_ *cobra.Command, _ []string) error {
	migrationnotes.BeforeCommands(VarStringDir, VarStringStyle)
	src := VarStringSrc
	dir := VarStringDir
	cache := VarBoolCache
	idea := VarBoolIdea
	style := VarStringStyle
	database := VarStringDatabase
	home := VarStringHome
	remote := VarStringRemote
	branch := VarStringBranch
	if len(remote) > 0 {
		repo, _ := file.CloneIntoGitHome(remote, branch)
		if len(repo) > 0 {
			home = repo
		}
	}
	if len(home) > 0 {
		pathx.RegisterGoctlHome(home)
	}
	cfg, err := config.NewConfig(style)
	if err != nil {
		return err
	}

	return fromDDLGorm(src, dir, cfg, cache, idea, database)
}

func fromDDLGorm(src, dir string, cfg *config.Config, cache, idea bool, database string) error {
	log := console.NewConsole(idea)
	src = strings.TrimSpace(src)
	if len(src) == 0 {
		return errors.New("expected path or path globbing patterns, but nothing found")
	}

	files, err := util.MatchFiles(src)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return errNotMatched
	}

	generator, err := gen.NewGormGenerator(dir, cfg, gen.WithConsoleOption(log))
	if err != nil {
		return err
	}

	for _, file := range files {
		err = generator.StartFromDDL(file, cache, database)
		if err != nil {
			return err
		}
	}
	return nil
}
