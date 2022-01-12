package job

import (
	"io/ioutil"
	"os"

	"github.com/labstack/echo/v4"
)

type JobResponse struct {
	JobId string `json:"jobId"`
	PID   string `json:"pid"`
}

func ListJobs(msgPath string, l echo.Logger) ([]JobResponse, error) {
	users := make([]JobResponse, 0)
	files, err := ioutil.ReadDir(msgPath)
	if err != nil {
		l.Warn(err)
		return users, err
	}

	for _, f := range files {
		if f.IsDir() {
			dat, err := os.ReadFile(msgPath + "/" + f.Name() + "/" + PIDFileName)
			if err == nil {
				users = append(
					users,
					JobResponse{
						JobId: f.Name(),
						PID:   string(dat),
					},
				)
			} else {
				users = append(
					users,
					JobResponse{
						JobId: f.Name(),
					},
				)
			}
		}
	}

	return users, nil
}
