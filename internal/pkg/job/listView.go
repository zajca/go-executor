package job

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/labstack/echo/v4"
)

type JobResponse struct {
	JobId string `json:"jobId"`
	PID   string `json:"pid"`
}

func ListJobs(msgPath string, l echo.Logger) ([]JobResponse, error) {
	files, err := ioutil.ReadDir(msgPath)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	users := make([]JobResponse, 0)

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
