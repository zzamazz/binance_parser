import { createWriteStream } from "fs";
import Axios from "axios";

const downloadFile = async (fileUrl: string, outputLocationPath: string) => {
    const writer = createWriteStream(outputLocationPath);

    return Axios({
        method: "get",
        url: fileUrl,
        responseType: "stream",
    }).then((response) => {
        return new Promise((resolve, reject) => {
            response.data.pipe(writer);
            let error: Error | null = null;
            writer.on("error", (err) => {
                error = err;
                writer.close();
                reject(err);
            });
            writer.on("close", () => {
                if (!error) {
                    resolve(true);
                }
            });
        });
    });
}
const unix_timestamp = async () => Math.floor(Date.now() / 1000);

export { downloadFile, unix_timestamp };