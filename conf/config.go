package conf

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

var Config config

type config struct {
	Debug bool  `yaml:"debug"`
	Otp   otp   `yaml:"otp"`
	UI    ui    `yaml:"ui"`
	Agent agent `yaml:"agent"`
	PG    pg    `yaml:"pg"`
}

type otp struct {
	Key string `yaml:"key"`
}

type ui struct {
	Host   string `yaml:"host"`
	Port   int    `yaml:"port"`
	Secret string `yaml:"secret"`
}

type agent struct {
	Port   int    `yaml:"port"`
	Secret string `yaml:"secret"`
}

type pg struct {
	Host     string `yaml:"host"`
	DB       string `yaml:"db"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

func init() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	home, err := os.UserHomeDir()

	if err == nil {
		path := home + "/.scepter/"
		if _, err := os.Stat(path); err == nil {
			viper.AddConfigPath(path)
		}
	}
	viper.AddConfigPath("./conf")

	viper.AutomaticEnv()
	viper.SetEnvPrefix("SCEPTER")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Fprintln(os.Stderr, err, viper.ConfigFileUsed())
		os.Exit(1)
	}
	fmt.Println("config from:", viper.ConfigFileUsed())

	err = viper.Unmarshal(&Config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
