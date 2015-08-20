package syslogoutput

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"io"
	"io/ioutil"
	"log/syslog"
	"strings"
)

type SyslogOutput struct {
	*SyslogOutputConfig
	Protocol string
	Host     string
	Tag      string
}

type SyslogOutputConfig struct {
	Protocol string `toml:"protocol"`
	Host     string `toml:"host"`
	Tag      string `toml:"tag"`
}

func (s *SyslogOutput) ConfigStruct() interface{} {
	return &SyslogOutputConfig{
		Protocol: "udp",
		Host:     "33.33.33.20:514",
		Tag:      "DefaultValue",
	}
}

func (s *SyslogOutput) Init(config interface{}) (err error) {
	s.SyslogOutputConfig = config.(*SyslogOutputConfig)

	switch s.SyslogOutputConfig.Protocol {
	case "udp":
	case "tcp":
	default:
		return fmt.Errorf("Value %s found, protocol must be 'udp' or 'tcp'", s.SyslogOutputConfig.Protocol)
	}

	hostParse := strings.Split(s.SyslogOutputConfig.Host, ":")
	if hostParse[0] == "" || hostParse[1] == "" {
		return fmt.Errorf("Host must be in <host>:<port> format.")
	}

	if s.SyslogOutputConfig.Tag == "" {
		return fmt.Errorf("Tag must be a valid string.")
	}

	return
}

func (s *SyslogOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	if or.Encoder() == nil {
		return errors.New("Encoder must be specified.")
	}

	var (
		e        error
		outBytes []byte
	)

	inChan := or.InChan()

	for pack := range inChan {
		outBytes, e = or.Encode(pack)
		pack.Recycle(e)
		if e != nil {
			or.LogError(e)
			continue
		}
		if outBytes == nil {
			continue
		}
		if e = s.tosyslog(or, outBytes); e != nil {
			or.LogError(e)
		}
	}
	return
}

func (s *SyslogOutput) tosyslog(or pipeline.OutputRunner, outBytes []byte) (err error) {
	var (
		reader     io.Reader
		readCloser io.ReadCloser
	)

	logger, err := syslog.Dial(s.SyslogOutputConfig.Protocol, s.SyslogOutputConfig.Host, syslog.LOG_LOCAL1, s.SyslogOutputConfig.Tag)
	if err != nil {
		fmt.Errorf("Could not dial remote syslog host: %s", err)

	}
	defer logger.Close()

	reader = bytes.NewReader(outBytes)
	readCloser = ioutil.NopCloser(reader)
	msg := new(bytes.Buffer)
	msg.ReadFrom(readCloser)
	msg_string := msg.String()

	logger.Info(msg_string)

	return err

}

func init() {
	pipeline.RegisterPlugin("SyslogOutput", func() interface{} {
		return new(SyslogOutput)
	})
}
