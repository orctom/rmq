package ui

import (
	"fmt"
	"net/http"

	"github.com/gorilla/sessions"
	"github.com/gorilla/websocket"
	prometheus "github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo-contrib/session"
	jwt "github.com/labstack/echo-jwt/v4"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog/log"

	"orctom.com/rmq/conf"
	"orctom.com/rmq/dist"
)

var (
	upgrader = websocket.Upgrader{}
)

func ws(c echo.Context) error {
	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return err
	}
	defer ws.Close()

	for {
		// Write
		err := ws.WriteMessage(websocket.TextMessage, []byte("Hello, Client!"))
		if err != nil {
			c.Logger().Error(err)
		}

		// Read
		_, msg, err := ws.ReadMessage()
		if err != nil {
			c.Logger().Error(err)
		}
		fmt.Printf("%s\n", msg)
	}
}

func Start() {
	e := echo.New()
	e.HideBanner = true

	e.Pre(middleware.RemoveTrailingSlash())
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		Skipper: func(c echo.Context) bool {
			return conf.P_SKIP_ACCESS_LOG.MatchString(c.Path())
		},
		LogURI:    true,
		LogStatus: true,
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			log.Info().
				Str("time", v.StartTime.Format(conf.DATETIME_FMT_ZONE)).
				Str("uri", v.URI).
				Str("remote", v.RemoteIP).
				Str("ua", v.UserAgent).
				Str("latency", v.Latency.Abs().String()).
				Int("status", v.Status).
				Msg("access")

			return nil
		},
	}))
	e.Use(middleware.Recover())
	e.Use(middleware.BodyLimit("50M"))
	e.Use(middleware.Decompress())
	e.Use(middleware.Gzip())
	e.Use(jwt.JWT([]byte(conf.Config.UI.Secret)))
	e.Use(session.Middleware(sessions.NewCookieStore([]byte(conf.Config.UI.Secret))))

	e.Use(prometheus.NewMiddleware("rmq"))
	e.GET("/metrics", prometheus.NewHandler())

	e.Use(middleware.StaticWithConfig(middleware.StaticConfig{
		HTML5:      true,
		Root:       "/",
		Browse:     true,
		Filesystem: http.FS(dist.FS),
	}))
	// e.Static("/", "../public")
	e.GET("/ws", ws)

	address := fmt.Sprintf("%s:%d", conf.Config.UI.Host, conf.Config.UI.Port)
	log.Info().Str("address", address).Msg("starting ui")
	log.Fatal().Err(e.Start(address)).Send()
}
