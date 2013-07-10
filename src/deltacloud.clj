(ns deltacloud
  (:require [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.string :refer [split]]
            [clojure.core.incubator :refer [-?> -?>>]]
            [slingshot.slingshot :refer [try+ throw+]]))

;;(declare ^:dynamic *kill-instance-when-finished*)

(def ^:dynamic *kill-instance-when-finished* true) 

(defrecord Instance [name state actions public_addresses connection])
(defrecord InstanceDefinition [name image_id hwp_memory])
(defrecord Connection [url user password])

(defn response->instance [r conn]
  (-> r :instance (assoc :connection conn) map->Instance))

(defn http-method [kw]
  (->> kw name symbol (ns-resolve 'clj-http.client)))

(defn request [conn http-method uri & [req]]
  (-?> (http-method (or (:href req) (format "%s/%s" (:url conn) uri))
                    (dissoc (merge req {:basic-auth [(:user conn) (:password conn)]
                                        :accept :json
                                        :content-type :json})
                            :href))
       :body
       json/read-json))

(defn instances
  "get all instances"
  [conn]
  (:instances (request conn http/get "instances")))

(defmacro defstates [m]
  `(do ~@(for [[k v] m]
          `(defn ~k [i#]
             (-> i# :state (= ~v))))))

(defstates {stopped? "STOPPED"
            running? "RUNNING"
            pending? "PENDING"})

(defn by-name [inst-name]
  #(= inst-name (:name %)))

(defn instance-by-name [conn inst-name]
  (->> conn instances (filter (by-name inst-name)) first))

(def ^{:doc "A set of properties for a small instance."}
  small-instance-properties
  {:hwp_cpus "2"
   :hwp_memory "256"})

(defn get-actions [i]
  (let [method-entry (fn [action]
                       [(-> action :rel keyword),
                        (partial request (:connection i)
                                 (-> action :method keyword http-method)
                                 nil
                                 {:href (:href action)})])]
    
    (->> i :actions (map method-entry) (into {}))))

(defn action-available-pred [action]
  #(->> % :actions (map :rel) (some #{action})))

(defn ip-address [inst]
  (-?>> inst :public_addresses
        (filter #(= (:type %) "ipv4"))
        first
        :address))

(defn refresh "Reloads the instance from deltacloud"
  [i]
  (let [conn (:connection i)]
    (-> conn
        (request http/get (format "instances/%s" (:id i))),
        (response->instance conn))))

(defn wait-for
  "Wait for pred to become true on instance i (refreshing
  periodically)"
  [i pred]
  (loop [i i]
    (if (pred i)
      i
      (do
        #_(prn (select-keys i [:name :state :public_addresses :actions]))
        (Thread/sleep 30000)
        (recur (refresh i))))))

(defn perform-action-wait
  "Performs action on instance i, waits for pred to become true."
  [i action pred]
  (let [avail-actions (get-actions i)]
    (assert (some #{action} (keys avail-actions))
            (format "%s not one of available actions on instance: %s"
                    action
                    (keys avail-actions)))
    (wait-for (-> action avail-actions .invoke (response->instance (:connection i)))
              pred)))

(defn create-instance
  "Creates an instance with the given connection, and
  instance definition. Returns the instance data as a record."
  [conn instance-definition]
  (-> (request conn http/post "instances" {:query-params instance-definition})
      (response->instance conn)
      (wait-for (action-available-pred "start"))))

(defn stop [i]
  (perform-action-wait i :stop stopped?))

(defn destroy [i]
  (perform-action-wait i :destroy nil?))

(defn unprovision "Whatever state the instance is in, destroy it"
  ([i]
     {:pre [(:connection i)]}
     (try+
      (cond (stopped? i) (destroy i)
            (running? i) (destroy (stop i))
            :else (unprovision (wait-for i
                                         (some-fn stopped? running?))))
      (catch Exception e (throw+ {:type ::unprovision-failed
                                  :instance i
                                  :cause e})))))

(defn start [i]
  (try+
   (perform-action-wait i :start (every-pred running? ip-address))
   (catch [:type ::timeout-error] e
     (unprovision (::instance e))
     (throw+ e))))

(defn provision
  "Create an instance, start it and wait for it to come up."
  [conn instance-definition]
  (start (create-instance conn instance-definition)))

(defn provision-all
  "Provision instances with given properties (a list of maps), in
   parallel. Returns the instances data from deltacloud, and the
   deltacloud connection."
  [conn instance-props]
  {:instances (->> (for [inst-prop instance-props]
                     (future (provision conn inst-prop)))
                   doall
                   (map deref))})

(defn unprovision-all
  "Destroy all the given instances, in parallel."
  [instances]
  (for [f (doall (for [i instances]
                   (future (unprovision i))))]
    (let [r (try+ (deref f)
                  (catch [:type ::unprovision-failed] e e))
          grouped (group-by :type r)]
      (if (seq (:type grouped))
        (throw+ {:type ::some-unprovisisons-failed
                 :results grouped})
        r))))

(defmacro with-instances "Executes body with bound instances."
  [instances-binding & body]
  `(let ~instances-binding
     (try
       ~@body
       (finally (when *kill-instance-when-finished*
                  (unprovision-all ~(first instances-binding)))))))

(defmacro with-instance [instance-binding & body]
  `(let ~instance-binding
     (try
       ~@body
       (finally (when *kill-instance-when-finished*
                  (unprovision ~(first instance-binding)))))))

